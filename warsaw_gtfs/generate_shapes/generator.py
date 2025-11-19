import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache
from itertools import pairwise, starmap
from math import inf

import routx

# TODO: Generate shape_dist_traveled
# TODO: Check that the nodes matched with stops are within reasonable distance
# TODO: Check that the shape of a leg is not unreasonably longer than a straight-line-path
# TODO: Add support for force-via

LatLon = tuple[float, float]


@dataclass
class MatchedStop:
    stop_id: str
    node_id: int


@dataclass
class LegRequest:
    from_: MatchedStop
    to: MatchedStop
    force_via: LatLon | None = None
    max_distance_ratio: float = inf


class ShapeGenerator:
    def __init__(
        self,
        stop_positions: Mapping[str, LatLon],
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

    def generate_shape(self, stops: Iterable[str]) -> list[LatLon]:
        matched_stops = self.match_stops_to_nodes(stops)
        leg_requests = self.generate_leg_requests(matched_stops)
        legs = self.generate_leg_shapes(leg_requests)
        return list(self.flatten_shape(legs))

    def match_stops_to_nodes(self, stop_ids: Iterable[str]) -> Iterable[MatchedStop]:
        return map(self.match_stop, stop_ids)

    def match_stop(self, stop_id: str) -> MatchedStop:
        return MatchedStop(stop_id, self.stop_to_node(stop_id))

    def generate_leg_requests(self, matched_stops: Iterable[MatchedStop]) -> Iterable[LegRequest]:
        return starmap(self.generate_leg_request, pairwise(matched_stops))

    def generate_leg_request(self, from_: MatchedStop, to: MatchedStop) -> LegRequest:
        # TODO: Add support for force-via
        # TODO: Add support for ratio override
        return LegRequest(from_, to)

    def generate_leg_shapes(self, requests: Iterable[LegRequest]) -> Iterable[Iterable[LatLon]]:
        return map(self.generate_leg_shape, requests)

    def generate_leg_shape(self, r: LegRequest) -> Iterable[LatLon]:
        if r.force_via is not None:
            raise NotImplementedError("TODO: Add support for LegRequest.force_via")

        stop_pair = (r.from_.stop_id, r.to.stop_id)
        if stop_pair in self.failed_pairs:
            node_ids = [r.from_.node_id, r.to.node_id]
        else:
            try:
                node_ids = self.graph.find_route(r.from_.node_id, r.to.node_id)
            except routx.StepLimitExceeded:
                self.failed_pairs.add(stop_pair)
                self.logger.error("No route exists between stops %s and %s", *stop_pair)
                node_ids = [r.from_.node_id, r.to.node_id]

            if r.max_distance_ratio != inf:
                raise NotImplementedError("TODO: Check leg to crow-flies distance ratio")

            for node_id in node_ids:
                node = self.graph[node_id]
                yield node.lat, node.lon

    def flatten_shape(self, legs: Iterable[Iterable[LatLon]]) -> Iterable[LatLon]:
        for leg_idx, leg in enumerate(legs):
            for pt_idx, pt in enumerate(leg):
                # Skip first point of every leg - it's the same as last point of previous leg -
                # except if it's the very first leg
                if pt_idx > 0 or leg_idx == 0:
                    yield pt
