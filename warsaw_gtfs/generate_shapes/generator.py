import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from itertools import pairwise, starmap
from math import inf, nan
from typing import NamedTuple, Self

import routx

# TODO: Check that the nodes matched with stops are within reasonable distance
# TODO: Check that the shape of a leg is not unreasonably longer than a straight-line-path
# TODO: Add support for force-via

StopIdSequence = tuple[str, int]
LatLon = tuple[float, float]


class LatLonDist(NamedTuple):
    lat: float
    lon: float
    total_distance: float = nan

    def with_distance_offset(self, offset: float) -> Self:
        new_dist = round(self.total_distance + offset, 6)
        return self._replace(total_distance=new_dist)


@dataclass
class MatchedStop:
    stop_id: str
    node_id: int
    stop_sequence: int | None


ShapeRequest = Iterable[StopIdSequence]


@dataclass
class ShapeResponse:
    points: list[LatLonDist] = field(default_factory=list[LatLonDist])
    distances: dict[int, float] = field(default_factory=dict[int, float])


@dataclass
class LegRequest:
    from_: MatchedStop
    to: MatchedStop
    max_distance_ratio: float = inf


@dataclass
class LegResponse:
    from_: MatchedStop
    to: MatchedStop
    points: list[LatLonDist]

    @classmethod
    def prepare(cls, points: list[LatLonDist], request: LegRequest) -> Self:
        return cls(request.from_, request.to, points)


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

    def generate_shape(self, stops: ShapeRequest) -> ShapeResponse:
        matched_stops = self.match_stops_to_nodes(stops)
        leg_requests = self.generate_leg_requests(matched_stops)
        legs = self.generate_leg_shapes(leg_requests)
        return self.flatten_shape(legs)

    def match_stops_to_nodes(self, stops: ShapeRequest) -> Iterable[MatchedStop]:
        return starmap(self.match_stop, stops)

    def match_stop(self, stop_id: str, stop_sequence: int) -> MatchedStop:
        return MatchedStop(stop_id, self.stop_to_node(stop_id), stop_sequence)

    def generate_leg_requests(self, matched_stops: Iterable[MatchedStop]) -> Iterable[LegRequest]:
        return starmap(self.generate_leg_request, pairwise(matched_stops))

    def generate_leg_request(self, from_: MatchedStop, to: MatchedStop) -> LegRequest:
        # TODO: Add support for force-via
        # TODO: Add support for ratio override
        return LegRequest(from_, to)

    def generate_leg_shapes(self, requests: Iterable[LegRequest]) -> Iterable[LegResponse]:
        return map(self.generate_leg_shape, requests)

    def generate_leg_shape(self, r: LegRequest) -> LegResponse:
        fallback_shape = list(self._nodes_to_points((r.from_.node_id, r.to.node_id)))
        shape = self._generate_leg_shape_unchecked(r.from_, r.to)
        if not shape:
            return LegResponse.prepare(fallback_shape, r)  # TODO: report routx failure

        if r.max_distance_ratio != inf:
            raise NotImplementedError("TODO: Check leg to crow-flies distance ratio")

        return LegResponse.prepare(shape, r)

    def flatten_shape(self, legs: Iterable[LegResponse]) -> ShapeResponse:
        r = ShapeResponse()
        dist_offset = 0.0

        for leg_idx, leg in enumerate(legs):
            # Save the traveled distance to the very first stop if not already
            if not r.distances:
                assert leg.from_.stop_sequence is not None, "first leg's from must be a stop"
                r.distances[leg.from_.stop_sequence] = 0.0

            # Skip first point of every leg - it's the same as last point of previous leg -
            # except if it's the very first leg
            pts_offset = 0 if leg_idx == 0 else 1
            assert leg.points[0].total_distance == 0.0
            r.points.extend(pt.with_distance_offset(dist_offset) for pt in leg.points[pts_offset:])

            # Save the distance traveled to the leg.to stop
            dist_offset = r.points[-1].total_distance
            if leg.to.stop_sequence is not None:
                r.distances[leg.to.stop_sequence] = dist_offset

        return r

    def _generate_leg_shape_unchecked(
        self,
        from_: MatchedStop,
        to: MatchedStop,
    ) -> list[LatLonDist]:
        stop_pair = (from_.stop_id, to.stop_id)
        if stop_pair in self.failed_pairs:
            return []
        else:
            try:
                nodes = self.graph.find_route(from_.node_id, to.node_id)
                return list(self._nodes_to_points(nodes))
            except routx.StepLimitExceeded:
                self.failed_pairs.add(stop_pair)
                self.logger.error("No route exists between stops %s and %s", *stop_pair)
                return []

    def _nodes_to_points(self, nodes: Iterable[int]) -> Iterable[LatLonDist]:
        dist = 0.0
        prev_lat_lon: LatLon | None = None
        for node_id in nodes:
            node = self.graph[node_id]
            if prev_lat_lon:
                dist += routx.earth_distance(*prev_lat_lon, node.lat, node.lon)
            yield LatLonDist(node.lat, node.lon, dist)
            prev_lat_lon = node.lat, node.lon
