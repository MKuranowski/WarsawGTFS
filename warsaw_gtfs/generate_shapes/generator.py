import logging
from collections.abc import Iterable, Mapping
from functools import lru_cache
from itertools import pairwise

import routx

# TODO: Generate shape_dist_traveled
# TODO: Check that the nodes matched with stops are within reasonable distance
# TODO: Check that the shape of a leg is not unreasonably longer than a straight-line-path
# TODO: Add support for force-via

Pos = tuple[float, float]


class ShapeGenerator:
    def __init__(
        self,
        stop_positions: Mapping[str, Pos],
        graph: routx.Graph,
        kd_tree: routx.KDTree | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger(type(self).__name__)

        self.stop_positions = stop_positions
        self.graph = graph
        self.kd_tree = kd_tree
        self.failed_pairs = set[tuple[str, str]]()

    @lru_cache(maxsize=None)
    def stop_to_node(self, stop_id: str) -> int:
        lat, lon = self.stop_positions[stop_id]
        if self.kd_tree:
            return self.kd_tree.find_nearest_node(lat, lon).id
        return self.graph.find_nearest_node(lat, lon).id

    def generate_shape(self, stops: Iterable[str]) -> list[Pos]:
        stops_and_nodes = self.match_stops_to_nodes(stops)
        legs = self.generate_leg_shapes(stops_and_nodes)
        return list(self.flatten_shape(legs))

    def match_stops_to_nodes(self, stops: Iterable[str]) -> Iterable[tuple[str, int]]:
        for stop in stops:
            yield stop, self.stop_to_node(stop)

    def generate_leg_shapes(
        self,
        stops_and_nodes: Iterable[tuple[str, int]],
    ) -> Iterable[Iterable[Pos]]:
        for (stop_a, node_a), (stop_b, node_b) in pairwise(stops_and_nodes):
            yield self.generate_leg_shape(stop_a, node_a, stop_b, node_b)

    def generate_leg_shape(
        self,
        stop_a: str,
        node_a: int,
        stop_b: str,
        node_b: int,
    ) -> Iterable[Pos]:
        if (stop_a, stop_b) in self.failed_pairs:
            node_ids = [node_a, node_b]
        try:
            node_ids = self.graph.find_route(node_a, node_b)
        except routx.StepLimitExceeded:
            self.failed_pairs.add((stop_a, stop_b))
            self.logger.error("No route exists between stops %s and %s", stop_a, stop_b)
            node_ids = [node_a, node_b]

        for node_id in node_ids:
            node = self.graph[node_id]
            yield node.lat, node.lon

    def flatten_shape(self, legs: Iterable[Iterable[Pos]]) -> Iterable[Pos]:
        for leg_idx, leg in enumerate(legs):
            for pt_idx, pt in enumerate(leg):
                # Skip first point of every leg - it's the same as last point of previous leg -
                # except if it's the very first leg
                if pt_idx > 0 or leg_idx == 0:
                    yield pt
