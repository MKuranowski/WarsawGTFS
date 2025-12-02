from collections import defaultdict
from collections.abc import Iterable, Mapping
from math import inf
from typing import Any, cast

import osmium
import osmium.filter
import osmium.osm
import routx
from impuls import DBConnection
from impuls import Task as ImpulsTask
from impuls import TaskRuntime
from impuls.resource import ManagedResource
from impuls.tools.geo import earth_distance_m
from impuls.tools.types import StrPath

from .config import GenerateConfig, GraphConfig, LoggingConfig
from .generator import ShapeGenerator
from .model import ForceVia, LatLon, RatioOverrides, TripsByStops, TripStops


class Task(ImpulsTask):
    """Generates shapes based on OSM data (and some manual curation)"""

    def __init__(
        self,
        graph: GraphConfig,
        gen: GenerateConfig,
        logging: LoggingConfig = LoggingConfig(),
    ) -> None:
        super().__init__(name=logging.task_name)
        self.graph = graph
        self.gen = gen
        self.logging = logging

    def execute(self, r: TaskRuntime) -> None:
        # 1. Get trips to process
        self.logger.info("Getting trips to process")
        trip_ids = self.get_trip_ids_to_process(r.db)
        trips_by_stops = self.group_trips_by_stops(r.db, trip_ids)

        # 2. (If overwrite) - Cleanup shapes
        if self.gen.overwrite:
            self.logger.info("Clearing any existing shapes")
            self.clean_overwritten_shapes(r.db, trip_ids)

        # 3. Create the graph and k-d tree
        self.logger.info("Loading OSM data")
        generator = self.create_generator(r.db, r.resources)

        # 4. Generate and save the shapes
        self.generate_shapes(r.db, trips_by_stops, generator)
        self.logger.info("Shape generation complete")

    def get_trip_ids_to_process(self, db: DBConnection) -> list[str]:
        """Returns a list of trip_ids for which this task should run."""
        route_ids = list(self.gen.routes.find_ids(db))
        trip_query = "SELECT trip_id FROM trips WHERE route_id = ?"
        if not self.gen.overwrite:
            trip_query += " AND shape_id IS NULL"
        return [
            cast(str, i[0])
            for route_id in route_ids
            for i in db.raw_execute(trip_query, (route_id,))
        ]

    def group_trips_by_stops(self, db: DBConnection, trip_ids: Iterable[str]) -> TripsByStops:
        """Groups trips which should have the same shape -
        by the same sequence of (stop_id, stop_sequence) pairs.
        """

        trips_by_stops = defaultdict[TripStops, list[str]](list)
        for trip_id in trip_ids:
            stops = tuple(
                (cast(str, i[0]), cast(int, i[1]))
                for i in db.raw_execute(
                    "SELECT stop_id, stop_sequence FROM stop_times "
                    "WHERE trip_id = ? ORDER BY stop_sequence ASC",
                    (trip_id,),
                )
            )
            trips_by_stops[stops].append(trip_id)
        return trips_by_stops

    def clean_overwritten_shapes(self, db: DBConnection, trip_ids: Iterable[str]) -> None:
        """Removes shapes which would have been overwritten by this step."""
        shape_id_params = [(i,) for i in self.get_shape_ids_used_by_trips(db, trip_ids)]
        with db.transaction():
            db.raw_execute_many(
                "UPDATE trips SET shape_id = NULL WHERE shape_id = ?",
                shape_id_params,
            )
            db.raw_execute_many("DELETE FROM shapes WHERE shape_id = ?", shape_id_params)

    @staticmethod
    def get_shape_ids_used_by_trips(db: DBConnection, trip_ids: Iterable[str]) -> set[str]:
        """Returns a set of shape_ids used by the provided trip_ids."""
        trip_ids = set(trip_ids)
        shape_ids = set[str]()
        for row in db.raw_execute("SELECT trip_id, shape_id FROM trips"):
            trip_id = cast(str, row[0])
            shape_id = cast(str | None, row[1])
            if shape_id and trip_id in trip_ids:
                shape_ids.add(shape_id)
        return shape_ids

    def create_generator(
        self,
        db: DBConnection,
        resources: Mapping[str, ManagedResource],
    ) -> ShapeGenerator:
        """Creates a ShapeGenerator instance for the current config and provided runtime."""
        self.logger.debug("Building routing graph")
        osm_file_path = resources[self.graph.osm_resource].stored_at
        graph = routx.Graph()
        graph.add_from_osm_file(osm_file_path, self.graph.profile, bbox=self.graph.bbox)

        self.logger.debug("Building k-d tree")
        kd_tree = routx.KDTree.build(graph)

        self.logger.debug("Building stop position lookup table")
        stop_positions = self.load_stop_positions(db, osm_file_path)

        self.logger.debug("Loading manual curation")
        ratio_overrides, force_via = self.load_curation(resources)

        return ShapeGenerator(
            stop_positions,
            graph,
            kd_tree,
            max_stop_to_node_distance=self.graph.max_stop_to_node_distance,
            ratio_overrides=ratio_overrides,
            force_via=force_via,
            logger=self.logger,
            logging_config=self.logging,
        )

    def load_curation(
        self,
        resources: Mapping[str, ManagedResource],
    ) -> tuple[RatioOverrides, ForceVia]:
        """Loads curated overrides for the ShapeGenerator."""
        if not self.graph.curation_resource:
            return {}, {}

        curation = resources[self.graph.curation_resource].json()
        ratio_overrides = self.load_ratio_overrides(curation)
        force_via = self.load_force_via(curation)
        return ratio_overrides, force_via

    @staticmethod
    def load_ratio_overrides(curation: Any) -> RatioOverrides:
        return {(i["from"], i["to"]): i["ratio"] for i in curation["ratio_overrides"]}

    @staticmethod
    def load_force_via(curation: Any) -> ForceVia:
        return {(i["from"], i["to"]): tuple(i["via"]) for i in curation["force_via"]}

    def load_stop_positions(self, db: DBConnection, osm_file: StrPath) -> dict[str, LatLon]:
        """Returns a mapping from stop_ids to its positions, based on data
        currently stored in the database, and overrides from the OSM file.
        """

        positions = self.load_stop_positions_from_db(db)
        from_osm = set[str]()

        for osm_stop_id, lat, lon in self.load_stop_positions_from_osm(osm_file):
            original_position = positions.get(osm_stop_id)
            dist = earth_distance_m(lat, lon, *original_position) if original_position else inf
            if dist < self.graph.max_stop_to_node_distance:
                from_osm.add(osm_stop_id)
                positions[osm_stop_id] = lat, lon

        self.logger.info(
            "Overridden %d / %d (%.2f %%) stop positions from OSM",
            len(from_osm),
            len(positions),
            100 * len(from_osm) / len(positions),
        )
        return positions

    @staticmethod
    def load_stop_positions_from_db(db: DBConnection) -> dict[str, tuple[float, float]]:
        return {
            cast(str, i[0]): (cast(float, i[1]), cast(float, i[2]))
            for i in db.raw_execute("SELECT stop_id, lat, lon FROM stops")
        }

    @staticmethod
    def load_stop_positions_from_osm(osm_file: StrPath) -> Iterable[tuple[str, float, float]]:
        fp = (
            osmium.FileProcessor(osm_file)
            .with_filter(osmium.filter.EntityFilter(osmium.osm.NODE))
            .with_filter(osmium.filter.TagFilter(("public_transport", "stop_position")))
            .with_filter(osmium.filter.KeyFilter(("network")))
        )
        for node in fp:
            assert isinstance(node, osmium.osm.Node)
            network = node.tags.get("network") or ""
            stop_id = node.tags.get("ref:wtp") or node.tags.get("ref") or ""
            if "ZTM Warszawa" in network and stop_id:
                yield stop_id, node.lat, node.lon

    def generate_shapes(
        self,
        db: DBConnection,
        trips_by_stops: TripsByStops,
        generator: ShapeGenerator,
    ) -> None:
        """Generates and saves shapes for the provided grouped trips using the generator."""
        with db.transaction():
            for i, (stops, trips) in enumerate(trips_by_stops.items()):
                if i % 100 == 0:
                    self.logger.info(
                        "Generated %.2f %% (%d/%d) shapes",
                        100 * i / len(trips_by_stops),
                        i,
                        len(trips_by_stops),
                    )

                shape_id = f"{self.gen.shape_id_prefix}{i}"
                shape = generator.generate_shape(stops)
                db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))
                db.raw_execute_many(
                    "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        (shape_id, i, lat, lon, dist)
                        for i, (lat, lon, dist) in enumerate(shape.points)
                    ),
                )
                db.raw_execute_many(
                    "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
                    ((shape_id, trip_id) for trip_id in trips),
                )
                db.raw_execute_many(
                    "UPDATE stop_times SET shape_dist_traveled = ? "
                    "WHERE trip_id = ? AND stop_sequence = ?",
                    (
                        (dist, trip_id, seq)
                        for seq, dist in shape.distances.items()
                        for trip_id in trips
                    ),
                )
