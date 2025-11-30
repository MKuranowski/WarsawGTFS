import json
import logging
from collections.abc import Iterable, Mapping
from functools import lru_cache
from itertools import pairwise, starmap
from math import inf
from pathlib import Path

import routx

from .config import LoggingConfig
from .model import (
    ForceVia,
    ForceViaPoint,
    LatLon,
    LatLonDist,
    LegRequest,
    LegResponse,
    MatchedStop,
    RatioOverrides,
    ShapeRequest,
    ShapeResponse,
)

# TODO: Check that the nodes matched with stops are within reasonable distance

MAX_DISTANCE_RATIO = 3.5
MAX_DISTANCE_RATIO_IN_SAME_GROUP = 7.0


class ShapeGenerator:
    def __init__(
        self,
        stop_positions: Mapping[str, LatLon],
        graph: routx.Graph,
        kd_tree: routx.KDTree,
        logger: logging.Logger | None = None,
        ratio_overrides: RatioOverrides | None = None,
        force_via: ForceVia | None = None,
        logging_config: LoggingConfig = LoggingConfig(),
    ) -> None:
        self.logger = logger or logging.getLogger(type(self).__name__)

        self.stop_positions = stop_positions
        self.graph = graph
        self.kd_tree = kd_tree
        self.failed_pairs = set[tuple[str, str]]()
        self.ratio_overrides = ratio_overrides or {}
        self.force_via = {
            stop_pair: ForceViaPoint(lat, lon)
            for stop_pair, (lat, lon) in (force_via or {}).items()
        }

        if logging_config.dump_errors:
            self.shape_err_dir = Path(logging_config.dump_errors)
            self.shape_err_dir.mkdir(exist_ok=True)
            if logging_config.clean_error_dir:
                for f in self.shape_err_dir.glob("*.geojson"):
                    f.unlink()
        else:
            self.shape_err_dir = None

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
        for a, b in pairwise(matched_stops):
            yield from self.generate_leg_request(a, b)

    def generate_leg_request(self, from_: MatchedStop, to: MatchedStop) -> list[LegRequest]:
        max_distance_ratio = self._get_max_distance_ratio(from_, to)
        force_via = self.force_via.get((from_.stop_id, to.stop_id))
        if force_via:
            force_via_fake_stop = MatchedStop(
                stop_id=f"via-{from_.stop_id}-{to.stop_id}",
                node_id=force_via.get_node_id(self.kd_tree),
            )
            return [
                LegRequest(from_, force_via_fake_stop, max_distance_ratio),
                LegRequest(force_via_fake_stop, to, max_distance_ratio),
            ]
        else:
            return [LegRequest(from_, to, max_distance_ratio)]

    def generate_leg_shapes(self, requests: Iterable[LegRequest]) -> Iterable[LegResponse]:
        return map(self.generate_leg_shape, requests)

    def generate_leg_shape(self, r: LegRequest) -> LegResponse:
        stop_pair = (r.from_.stop_id, r.to.stop_id)
        fallback_shape = list(self._nodes_to_points((r.from_.node_id, r.to.node_id)))
        shape = self._generate_leg_shape_unchecked(r)
        if not shape:
            return LegResponse.prepare(fallback_shape, r)

        if r.max_distance_ratio != inf:
            crow_flies_distance = fallback_shape[-1].distance
            shape_distance = shape[-1].distance
            distance_ratio = shape_distance / crow_flies_distance if crow_flies_distance else 1.0
            if distance_ratio > r.max_distance_ratio:
                self.failed_pairs.add(stop_pair)
                self.logger.error(
                    "Shape between %s and %s is too long - ratio is %.3f (max allowed is %.3f)",
                    *stop_pair,
                    distance_ratio,
                    r.max_distance_ratio,
                )
                self._report_failure(
                    r,
                    shape,
                    error="too_long",
                    ratio=round(distance_ratio, 4),
                    max_ratio=r.max_distance_ratio,
                )
                return LegResponse.prepare(fallback_shape, r)

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
            assert leg.points[0].distance == 0.0
            r.points.extend(pt.with_distance_offset(dist_offset) for pt in leg.points[pts_offset:])

            # Save the distance traveled to the leg.to stop
            dist_offset = r.points[-1].distance
            if leg.to.stop_sequence is not None:
                r.distances[leg.to.stop_sequence] = dist_offset

        return r

    def _get_max_distance_ratio(self, from_: MatchedStop, to: MatchedStop) -> float:
        override_from_key = from_.stop_id.partition(":")[0]
        override_to_key = to.stop_id.partition(":")[0]
        override_key = (override_from_key, override_to_key)
        if overridden := self.ratio_overrides.get(override_key):
            return overridden

        in_same_group = from_.stop_id[:4] == to.stop_id[:4]
        if in_same_group:
            return MAX_DISTANCE_RATIO_IN_SAME_GROUP

        return MAX_DISTANCE_RATIO

    def _generate_leg_shape_unchecked(self, r: LegRequest) -> list[LatLonDist]:
        stop_pair = (r.from_.stop_id, r.to.stop_id)
        if stop_pair in self.failed_pairs:
            return []
        else:
            try:
                nodes = self.graph.find_route(r.from_.node_id, r.to.node_id)
                return list(self._nodes_to_points(nodes))
            except routx.StepLimitExceeded:
                self.failed_pairs.add(stop_pair)
                self.logger.error("No route exists between stops %s and %s", *stop_pair)
                self._report_failure(
                    r,
                    self._nodes_to_points((r.from_.node_id, r.to.node_id)),
                    error="no_route",
                )
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

    def _report_failure(
        self,
        r: LegRequest,
        s: Iterable[LatLonDist],
        **properties: str | int | float,
    ) -> None:
        if self.shape_err_dir is None:
            return

        properties["from_stop_id"] = r.from_.stop_id
        properties["from_node_id"] = r.from_.node_id
        properties["to_stop_id"] = r.to.stop_id
        properties["to_node_id"] = r.to.node_id

        geojson = {
            "type": "Feature",
            "properties": properties,
            "geometry": {
                "type": "LineString",
                "coordinates": [[pt.lon, pt.lat] for pt in s],
            },
        }

        file = self.shape_err_dir / f"{r.from_.stop_id}__{r.to.stop_id}.geojson"
        with file.open("w", encoding="utf-8") as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)
