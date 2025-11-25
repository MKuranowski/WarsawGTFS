from collections import defaultdict
from collections.abc import Iterable, Mapping
from math import inf
from typing import cast

import osmium
import osmium.filter
import osmium.osm
import routx
from impuls import DBConnection, Task, TaskRuntime, selector
from impuls.resource import ManagedResource
from impuls.tools.geo import earth_distance_m
from impuls.tools.types import StrPath

from .generator import ShapeGenerator, StopIdSequence

MAX_DISTANCE_TO_OSM_STOP_POSITION_M = 100.0


class GenerateShapes(Task):
    def __init__(
        self,
        routes: selector.Routes,
        osm_resource: str,
        profile: routx.OsmProfile | routx.OsmCustomProfile,
        bbox: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0),
        overwrite: bool = False,
        shape_id_prefix: str = "",
        ratio_override_resource: str = "",
        force_via_resource: str = "",
        dump_errors: bool = False,
        task_name: str | None = None,
    ) -> None:
        super().__init__(name=task_name)
        self.routes = routes
        self.osm_resource = osm_resource
        self.profile = profile
        self.bbox = bbox
        self.overwrite = overwrite
        self.shape_id_prefix = shape_id_prefix
        self.ratio_override_resource = ratio_override_resource
        self.force_via_resource = force_via_resource
        self.dump_errors = dump_errors

    def execute(self, r: TaskRuntime) -> None:
        # 1. Get trips to process
        self.logger.info("Getting trips to process")
        trip_ids = self.get_trip_ids_to_process(r.db)
        trips_by_stops = self.group_trips_by_stops(r.db, trip_ids)

        # 2. (If overwrite) - Cleanup shapes
        if self.overwrite:
            self.logger.info("Clearing any existing shapes")
            self.clean_overwritten_shapes(r.db, trip_ids)

        # 3. Create the graph and k-d tree
        self.logger.info("Loading OSM data")
        generator = self.create_generator(r.db, r.resources)

        # 4. Generate and save the shapes
        with r.db.transaction():
            for i, (stops, trips) in enumerate(trips_by_stops.items()):
                if i % 100 == 0:
                    self.logger.info(
                        "Generated %.2f %% (%d/%d) shapes",
                        100 * i / len(trips_by_stops),
                        i,
                        len(trips_by_stops),
                    )

                shape_id = f"{self.shape_id_prefix}{i}"
                shape = generator.generate_shape(stops)
                r.db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))
                r.db.raw_execute_many(
                    "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        (shape_id, i, lat, lon, dist)
                        for i, (lat, lon, dist) in enumerate(shape.points)
                    ),
                )
                r.db.raw_execute_many(
                    "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
                    ((shape_id, trip_id) for trip_id in trips),
                )
                r.db.raw_execute_many(
                    "UPDATE stop_times SET shape_dist_traveled = ? "
                    "WHERE trip_id = ? AND stop_sequence = ?",
                    (
                        (dist, trip_id, seq)
                        for seq, dist in shape.distances.items()
                        for trip_id in trips
                    ),
                )

        self.logger.info("Shape generation complete")

    def group_trips_by_stops(
        self,
        db: DBConnection,
        trip_ids: Iterable[str],
    ) -> defaultdict[tuple[StopIdSequence, ...], list[str]]:
        trips_by_stops = defaultdict[tuple[StopIdSequence, ...], list[str]](list)
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

    def get_trip_ids_to_process(self, db: DBConnection) -> list[str]:
        route_ids = list(self.routes.find_ids(db))
        trip_query = "SELECT trip_id FROM trips WHERE route_id = ?"
        if not self.overwrite:
            trip_query += " AND shape_id IS NULL"
        return [
            cast(str, i[0])
            for route_id in route_ids
            for i in db.raw_execute(trip_query, (route_id,))
        ]

    def clean_overwritten_shapes(self, db: DBConnection, trip_ids: Iterable[str]) -> None:
        shape_id_params = [(i,) for i in self.get_shape_ids_used_by_trips(db, trip_ids)]
        with db.transaction():
            db.raw_execute_many(
                "UPDATE trips SET shape_id = NULL WHERE shape_id = ?",
                shape_id_params,
            )
            db.raw_execute_many("DELETE FROM shapes WHERE shape_id = ?", shape_id_params)

    @staticmethod
    def get_shape_ids_used_by_trips(db: DBConnection, trip_ids: Iterable[str]) -> set[str]:
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
        self.logger.debug("Building routing graph")
        osm_file_path = resources[self.osm_resource].stored_at
        graph = routx.Graph()
        graph.add_from_osm_file(osm_file_path, self.profile, bbox=self.bbox)

        self.logger.debug("Building k-d tree")
        kd_tree = routx.KDTree.build(graph)

        self.logger.debug("Building stop position lookup table")
        stop_positions = self.load_stop_positions(db, osm_file_path)

        return ShapeGenerator(
            stop_positions,
            graph,
            kd_tree,
            logger=self.logger,
            ratio_overrides=self.load_ratio_overrides(resources),
            force_via=self.load_force_via(resources),
            dump_errors=self.dump_errors,
        )

    def load_ratio_overrides(
        self,
        resources: Mapping[str, ManagedResource],
    ) -> dict[tuple[str, str], float]:
        if not self.ratio_override_resource:
            return {}
        return {
            (i["from"], i["to"]): i["ratio"] for i in resources[self.ratio_override_resource].json()
        }

    def load_force_via(
        self,
        resources: Mapping[str, ManagedResource],
    ) -> dict[tuple[str, str], tuple[float, float]]:
        if not self.force_via_resource:
            return {}
        return {
            (i["from"], i["to"]): tuple(i["via"]) for i in resources[self.force_via_resource].json()
        }

    def load_stop_positions(
        self,
        db: DBConnection,
        osm_file: StrPath,
    ) -> dict[str, tuple[float, float]]:
        positions = self.load_stop_positions_from_db(db)
        from_osm = set[str]()

        for osm_stop_id, lat, lon in self.load_stop_positions_from_osm(osm_file):
            original_position = positions.get(osm_stop_id)
            dist = earth_distance_m(lat, lon, *original_position) if original_position else inf
            if dist < MAX_DISTANCE_TO_OSM_STOP_POSITION_M:
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
            networks = (node.tags.get("network") or "").split(";")
            stop_id = node.tags.get("ref:wtp") or node.tags.get("ref") or ""
            if "ZTM Warszawa" in networks and stop_id:
                yield stop_id, node.lat, node.lon
