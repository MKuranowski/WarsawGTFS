import logging
import shutil
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple, cast

import pyroutelib3
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Route
from pyroutelib3.protocols import Position

BBOX = "20.5,51.9,21.5,52.5"
TAG_FILTER = ["w/highway", "r/type=restriction"]


class StopTime(NamedTuple):
    sequence: int
    stop_id: str


StopTimes = tuple[StopTime, ...]


class Trip(NamedTuple):
    id: str
    stop_times: StopTimes


class StopDistance(NamedTuple):
    sequence: int
    distance: float


class Shape(NamedTuple):
    shape_id: str
    stop_distances: list[StopDistance]


@dataclass
class ShapeGeneratorConfig:
    id: str
    route_type: Route.Type
    osm_resource: str
    osm_profile: pyroutelib3.osm.Profile
    trim_osm_with_osmium: bool = False


class ShapeGenerator:
    def __init__(
        self,
        id: str,
        graph: pyroutelib3.osm.Graph,
        kd_tree: pyroutelib3.KDTree[pyroutelib3.osm.GraphNode],
        stop_positions: dict[str, Position],
    ) -> None:
        self.logger = logging.getLogger(f"ShapeGenerator.{id}")
        self.graph = graph
        self.kd_tree = kd_tree
        self.stop_positions = stop_positions
        self.prefix = f"generated:{id}:"
        self.counter = 0
        self.stop_cache = dict[str, int]()
        self.shape_cache = dict[StopTimes, Shape]()

    def get_shape(self, db: DBConnection, stop_times: StopTimes) -> Shape:
        return self.shape_cache.get(stop_times) or self.generate_shape(db, stop_times)

    def generate_shape(self, db: DBConnection, stop_times: StopTimes) -> Shape:
        shape_id = self.get_next_shape_id()
        db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))

        stop_ids = (i.stop_id for i in stop_times)
        legs = (self.generate_leg_shape(a, b) for a, b in pairwise(stop_ids))

        distances = [StopDistance(stop_times[0].sequence, 0.0)]
        total_index = 0
        total_distance = 0.0
        last_pt: Position | None = None

        for to_stop_time, leg in zip(stop_times[1:], legs):
            # The first node will be the same as the last node of the previous leg -
            # skip it, unless it's the very first leg
            offset = 0 if last_pt is None else 1
            for pt in leg[offset:]:
                if last_pt:
                    total_distance += pyroutelib3.distance.haversine_earth_distance(pt, last_pt)
                db.raw_execute(
                    "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (shape_id, total_index, pt[0], pt[1], total_distance),
                )
                total_index += 1
                last_pt = pt

            distances.append(StopDistance(to_stop_time.sequence, total_distance))

        shape = Shape(shape_id, distances)
        self.shape_cache[stop_times] = shape
        return shape

    def generate_leg_shape(self, stop_a: str, stop_b: str) -> list[Position]:
        node_a = self.map_stop_to_node(stop_a)
        node_b = self.map_stop_to_node(stop_b)

        # TODO: Add support for shapes_force_via.json

        try:
            nodes = pyroutelib3.find_route_without_turn_around(self.graph, node_a, node_b)
        except pyroutelib3.StepLimitExceeded:
            nodes = []

        if not nodes:
            self.logger.error(
                "No route from %s (node %d) to %s (node %d)",
                stop_a,
                node_a,
                stop_b,
                node_b,
            )
            nodes = [node_a, node_b]

        # TODO: Check the total to crow-flies distance ratio:
        #       Within a single area this should be at most 7.
        #       otherwise this should be 3.

        # TODO: Add support for shape_override_ratios.json

        return [self.graph.nodes[i].position for i in nodes]

    def map_stop_to_node(self, stop_id: str) -> int:
        if cached := self.stop_cache.get(stop_id):
            return cached

        stop_position = self.stop_positions[stop_id]
        id = self.kd_tree.find_nearest_neighbor(stop_position).id
        self.stop_cache[stop_id] = id
        return id

    def get_next_shape_id(self) -> str:
        id = f"{self.prefix}{self.counter}"
        self.counter += 1
        return id


class GenerateShapes(Task):
    def __init__(self, configs: list[ShapeGeneratorConfig]) -> None:
        super().__init__()
        self.configs = configs

    def execute(self, r: TaskRuntime) -> None:
        for config in self.configs:
            generator = self.create_generator(config, r)
            self.generate(config.id, r.db, generator, config.route_type)

    def create_generator(self, config: ShapeGeneratorConfig, r: TaskRuntime) -> ShapeGenerator:
        self.logger.info("%s: loading geo data from %s", config.id, config.osm_resource)
        graph = self.load_graph(
            config.id,
            r.resources[config.osm_resource].stored_at,
            config.osm_profile,
            config.trim_osm_with_osmium,
        )
        kd_tree = self.build_kd_tree(config.id, graph)
        stop_positions = self.get_all_stop_positions(r.db)
        return ShapeGenerator(config.id, graph, kd_tree, stop_positions)

    def load_graph(
        self,
        id: str,
        osm_path: Path,
        profile: pyroutelib3.osm.Profile,
        trim_osm_with_osmium: bool = False,
    ) -> pyroutelib3.osm.Graph:
        if trim_osm_with_osmium:
            return self._trim_and_load_graph(id, osm_path, profile)
        else:
            return self._load_graph(id, osm_path, profile)

    def _trim_and_load_graph(
        self,
        id: str,
        osm_path: Path,
        profile: pyroutelib3.osm.Profile,
    ) -> pyroutelib3.osm.Graph:
        osmium = shutil.which("osmium")
        if osmium is None:
            raise RuntimeError("osmium-tool is not installed - unable to trim input file")

        with TemporaryDirectory(prefix="warsawgtfs-osm-trim") as temp_dir_str:
            temp_dir = Path(temp_dir_str)

            # NOTE: Experiments show that first trimming roads, then bbox is ever so slightly
            #       faster then going the other way around.
            roads_only_file = temp_dir / "roads.osm.pbf"
            trimmed_file = temp_dir / "trimmed.osm.pbf"

            self.logger.debug("%s: extracting roads from osm", id)
            subprocess.run(
                [
                    osmium,
                    "tags-filter",
                    "--no-progress",
                    "-o",
                    roads_only_file,
                    osm_path,
                    *TAG_FILTER,
                ],
                check=True,
            )

            self.logger.debug("%s: trimming osm", id)
            subprocess.run(
                [
                    osmium,
                    "extract",
                    "--bbox",
                    BBOX,
                    "--no-progress",
                    "-o",
                    trimmed_file,
                    roads_only_file,
                ],
                check=True,
            )

            return self._load_graph(id, trimmed_file, profile)

    def _load_graph(
        self,
        id: str,
        osm_path: Path,
        profile: pyroutelib3.osm.Profile,
    ) -> pyroutelib3.osm.Graph:
        with osm_path.open("rb") as f:
            self.logger.debug("%s: loading osm", id)
            return pyroutelib3.osm.Graph.from_file(profile, f)

    def build_kd_tree(
        self,
        id: str,
        g: pyroutelib3.osm.Graph,
    ) -> pyroutelib3.KDTree[pyroutelib3.osm.GraphNode]:
        self.logger.debug("%s: building a k-d tree", id)
        kd = pyroutelib3.KDTree[pyroutelib3.osm.GraphNode].build(
            i for i in g.nodes.values() if i.id == i.external_id
        )
        assert kd is not None
        return kd

    def get_all_stop_positions(self, db: DBConnection) -> dict[str, Position]:
        return {
            cast(str, i[0]): (cast(float, i[1]), cast(float, i[2]))
            for i in db.raw_execute("SELECT stop_id, lat, lon FROM stops")
        }

    def generate(
        self,
        id: str,
        db: DBConnection,
        generator: ShapeGenerator,
        route_type: Route.Type,
    ) -> None:
        self.logger.info("%s: generating shapes", id)
        with db.transaction():
            self.remove_existing_shapes(db, route_type)
            for trip_id, stop_times in self.get_trips_to_process(db, route_type):
                shape_id, distances = generator.get_shape(db, stop_times)
                self.assign_shape(db, trip_id, shape_id, distances)

    def remove_existing_shapes(self, db: DBConnection, type: Route.Type) -> None:
        db.raw_execute(
            "UPDATE trips SET shape_id = NULL "
            "WHERE (SELECT type FROM routes WHERE routes.route_id = trips.route_id) = ?",
            (type.value,),
        )
        db.raw_execute(
            "DELETE FROM shapes WHERE shape_id NOT IN (SELECT DISTINCT shape_id FROM trips)",
        )

    def get_trips_to_process(self, db: DBConnection, type: Route.Type) -> list[Trip]:
        trip_ids = list(
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT trip_id "
                "FROM trips "
                "LEFT JOIN routes ON (trips.route_id = routes.route_id) "
                "WHERE type = ?",
                (type.value,),
            )
        )

        return [
            Trip(
                trip_id,
                tuple(
                    StopTime(cast(int, i[0]), cast(str, i[1]))
                    for i in db.raw_execute(
                        "SELECT stop_sequence, stop_id FROM stop_times "
                        "WHERE trip_id = ? ORDER BY stop_sequence ASC",
                        (trip_id,),
                    )
                ),
            )
            for trip_id in trip_ids
        ]

    def assign_shape(
        self,
        db: DBConnection,
        trip_id: str,
        shape_id: str,
        distances: Iterable[StopDistance],
    ) -> None:
        db.raw_execute(
            "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
            (shape_id, trip_id),
        )
        db.raw_execute_many(
            "UPDATE stop_times SET shape_dist_traveled = ? "
            "WHERE trip_id = ? AND stop_sequence = ? ",
            ((i.distance, trip_id, i.sequence) for i in distances),
        )


DEFAULT_CONFIGS = [
    ShapeGeneratorConfig(
        "tram",
        Route.Type.TRAM,
        "tram_rail_shapes.osm",
        pyroutelib3.osm.RailwayProfile(penalties={"tram": 1}),
    ),
    ShapeGeneratorConfig(
        "rail",
        Route.Type.RAIL,
        "tram_rail_shapes.osm",
        pyroutelib3.osm.RailwayProfile(penalties={"rail": 1}),
    ),
    ShapeGeneratorConfig(
        "bus",
        Route.Type.BUS,
        "mazowieckie-latest.osm.pbf",
        pyroutelib3.osm.BusProfile(),
        trim_osm_with_osmium=True,
    ),
]
