import csv
import io
import json
import os
from logging import getLogger
from typing import (IO, Any, Dict, List, Literal, Mapping, Optional, Sequence,
                    Tuple)

import requests
from pyroutelib3 import Router, distHaversine

from ..const import DIR_SHAPE_ERR, HEADERS
from ..util import CsvWriter, ensure_dir_exists
from .const import (BUS_ROUTER_SETTINGS, GIST_FORCE_VIA, GIST_OVERRIDE_RATIOS,
                    OVERPASS_BUS_GRAPH, OVERPASS_STOPS_JSON, URL_OVERPASS,
                    URL_TRAM_TRAIN_GRAPH)
from .helpers import (_Pt, cache_retr, cache_save, simplify_line, time_limit,
                      total_length)
from .kdtree import KDTree

# cSpell: words kdtree retr rnodes


def get_force_via() -> Dict[Tuple[str, str], Tuple[float, float]]:
    """Gets via points for some shapes between given stops"""
    with requests.get(GIST_FORCE_VIA) as req:
        req.raise_for_status()
        return {
            (i["from"], i["to"]): tuple(i["via"])
            for i in req.json()
        }  # type: ignore


def get_override_ratios() -> Dict[Tuple[str, str], float]:
    """Gets via points for some shapes between given stops"""
    with requests.get(GIST_OVERRIDE_RATIOS) as req:
        req.raise_for_status()
        return {
            (i["from"], i["to"]): i["ratio"]
            for i in req.json()
        }  # type: ignore


class Shaper:
    def __init__(self, simplify: bool = True) -> None:
        self.simplify = simplify
        self.logger = getLogger("WarsawGTFS.Shaper")

        # External data
        self.override_ratios = get_override_ratios()
        self.force_via = get_force_via()

        # Make routers
        self.bus_router = self._make_router("bus")
        self.tram_router = self._make_router("tram")
        self.train_router = self._make_router("train")

        # Make KD-trees for nn lookups
        self.bus_kdtree = self._make_kdtree("bus")
        self.tram_kdtree = self._make_kdtree("tram")
        self.train_kdtree = self._make_kdtree("train")

        # Make stop_id → osm node lookup table
        self.bus_cached_stop_lookup: Dict[str, int] = {}
        self.tram_cached_stop_lookup: Dict[str, int] = {}
        self.train_cached_stop_lookup: Dict[str, int] = {}

        # Other used variables
        self.written_shapes: Dict[str, Mapping[int, float]] = {}
        self.dump_shape_issues = True

        # Pre-cache ZTM stop to OSM ID mapping
        self._load_osm_stops()

        # Variables set by the caller in other functions
        self.stop_data: Dict[str, Dict[str, Any]]
        self.file_obj: IO[str]
        self.writer: CsvWriter

    def __bool__(self) -> Literal[True]:
        return True

    # Getters

    def _router(self, transport: str) -> Router:
        """Returns the Router for a specific transport type"""
        if transport in {"bus", "3"}:
            return self.bus_router
        elif transport in {"tram", "0"}:
            return self.tram_router
        elif transport in {"train", "2"}:
            return self.train_router
        else:
            raise ValueError(f"Unknown transport type for shape generation: {transport}")

    def _kdtree(self, transport: str) -> KDTree:
        """Returns the KDTree for a specific transport type"""
        if transport in {"bus", "3"}:
            return self.bus_kdtree
        elif transport in {"tram", "0"}:
            return self.tram_kdtree
        elif transport in {"train", "2"}:
            return self.train_kdtree
        else:
            raise ValueError(f"Unknown transport type for shape generation: {transport}")

    def _cached_stop_lookup(self, transport: str) -> Dict[str, int]:
        """Returns the stop_id → osm_node lookup table for a specific transport type"""
        if transport in {"bus", "3"}:
            return self.bus_cached_stop_lookup
        elif transport in {"tram", "0"}:
            return self.tram_cached_stop_lookup
        elif transport in {"train", "2"}:
            return self.train_cached_stop_lookup
        else:
            raise ValueError(f"Unknown transport type for shape generation: {transport}")

    # External data loading

    def _get_bus_graph(self) -> IO[bytes]:
        """Retrieves the OSM graph for busses. First checks cache, then asks Overpass."""
        cached_name = "bus_graph.osm"
        cached_file = cache_retr(cached_name)

        if cached_file is not None:
            # Return the graph if it's cached
            self.logger.debug("OSM Bus Graph is loaded from cache")
            return cached_file

        else:
            self.logger.debug("OSM Bus Graph is loaded from Overpass API")
            buffer = io.BytesIO()

            # Make query to Overpass
            with requests.get(URL_OVERPASS, params={"data": OVERPASS_BUS_GRAPH}) as resp:
                resp.raise_for_status()
                for chunk in resp.iter_content(1024 * 16):
                    buffer.write(chunk)

            # Write to cache
            buffer.seek(0)
            cache_save(cached_name, buffer)
            buffer.seek(0)

            return buffer

    @staticmethod
    def _get_tramrail_graph() -> IO[bytes]:
        """Retrieves URL_TRAM_TRAIN_GRAPH"""
        temp_buffer = io.BytesIO()

        with requests.get(URL_TRAM_TRAIN_GRAPH, stream=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(1024 * 128):
                temp_buffer.write(chunk)

        temp_buffer.seek(0)
        return temp_buffer

    def _make_router(self, transport: Literal["bus", "tram", "train"]) -> Router:
        """Creates (and returns) a router for a specific transport type"""
        self.logger.info(f"Making router for {transport}")

        # Set per-type variables
        if transport == "bus":
            router_type = BUS_ROUTER_SETTINGS
            temp_buffer = self._get_bus_graph()

        else:
            router_type = transport
            temp_buffer = self._get_tramrail_graph()

        # Create the router
        try:
            router = Router(
                transport=router_type,
                localfile=temp_buffer,  # type: ignore
                localfileType="xml",
            )

        # Close the temporary buffer
        finally:
            temp_buffer.close()

        return router

    def _make_kdtree(self, transport: Literal["bus", "tram", "train"]) -> KDTree:
        """Creates (and returns) a KDTree for a specific transport type"""
        self.logger.debug(f"Making KD-Tree for {transport}")
        return KDTree.build_from_dict(self._router(transport).rnodes, 32)

    def _load_osm_stops(self) -> None:
        """Saves to `self` a mapping from ZTM stop ids to OSM element ids."""
        cached_name = "stop_lookups.json"
        cached_file = cache_retr(cached_name)

        if cached_file is not None:
            # Try to read osm stop mapping from a cached file
            cached_content = cached_file.read().decode("ascii")
            self.bus_cached_stop_lookup = json.loads(cached_content)

        else:
            # Make query to Overpass
            with requests.get(URL_OVERPASS, params={"data": OVERPASS_STOPS_JSON}) as resp:
                resp.raise_for_status()

                # Iterate over every stop_position
                for element in resp.json()["elements"]:
                    stop_ref = element.get("tags", {}).get("ref")
                    no_bus = element.get("tags", {}).get("bus") == "no"

                    if stop_ref and not no_bus:
                        self.bus_cached_stop_lookup[stop_ref] = element["id"]

            # Cache stop_lookup
            stop_lookups_json = json.dumps(self.bus_cached_stop_lookup, indent=2).encode("ascii")
            cache_save(cached_name, stop_lookups_json)

    @staticmethod
    def _dump_shape_err(from_stop: str, to_stop: str, from_node: int, to_node: int,
                        route: List[_Pt], status: str) -> None:
        """Dumps info about failed shape creation to DIR_SHAPE_ERR"""
        target_file = os.path.join(DIR_SHAPE_ERR, f"{from_stop}-{to_stop}.json")

        if os.path.exists(target_file):
            return

        err_obj = {
            "type": "FeatureCollection", "features": [{
                "type": "Feature",
                "properties": {
                    "stop_start": from_stop,
                    "stop_end": to_stop,
                    "node_start": from_node,
                    "node_end": to_node,
                    "status": status,
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[i[1], i[0]] for i in route]
                }
            }]
        }

        with open(target_file, mode="w") as f:
            json.dump(err_obj, f, indent=2)

    # Generating route between 2 stops

    def get_node(self, stop_id: str, transport: str) -> int:
        """
        Finds the node ID nearest to stop with given ID.
        Lookup is preformed on the graph corresponding to provided transport type.
        """
        router = self._router(transport)
        kdtree = self._kdtree(transport)
        cached_stop_lookups = self._cached_stop_lookup(transport)

        # First, check if this stop_is was already cached
        cached_id = cached_stop_lookups.get(stop_id)
        if cached_id is not None and cached_id in router.rnodes:
            return cached_id

        # Get stop poisition
        stop_info = self.stop_data[stop_id]
        lat = stop_info["stop_lat"]
        lon = stop_info["stop_lon"]

        assert isinstance(lat, float)
        assert isinstance(lon, float)

        # Search for NN in the KDTree
        nn = kdtree.search_nn((lat, lon))

        # Cache lookup
        cached_stop_lookups[stop_id] = nn.id

        return nn.id

    def staright_line(self, stop1: str, stop2: str) -> List[_Pt]:
        """Generates a straight line between 2 stops"""
        stop1_data = self.stop_data[stop1]
        stop1_lat = stop1_data["stop_lat"]
        stop1_lon = stop1_data["stop_lon"]

        stop2_data = self.stop_data[stop2]
        stop2_lat = stop2_data["stop_lat"]
        stop2_lon = stop2_data["stop_lon"]

        assert isinstance(stop1_lat, float)
        assert isinstance(stop1_lon, float)
        assert isinstance(stop2_lat, float)
        assert isinstance(stop2_lon, float)

        return [(stop1_lat, stop1_lon), (stop2_lat, stop2_lon)]

    @staticmethod
    def do_route(router: Router, start: int, end: int, via: Optional[int] = None) \
            -> Tuple[str, List[int]]:
        """
        If via is None, delegates execution to router.doRoute(start, end).
        Otherwise calls router.doRoute twice and then combines both results.
        """
        # No via point - simple doRoute
        if via is None:
            with time_limit(10):
                return router.doRoute(start, end)

        # Via point - do search on both legs
        else:
            with time_limit(10):
                s1, r1 = router.doRoute(start, via)
            with time_limit(10):
                s2, r2 = router.doRoute(via, end)

            if s1 != "success":
                return (s1 + "_to_via"), []
            elif s2 != "success":
                return (s2 + "_from_via"), []
            else:
                return "success", r1 + r2[1:]

    def _calculate_ratio(self, from_stop: str, to_stop: str, route: List[_Pt],
                         straight_route: List[_Pt]) -> Optional[str]:
        """
        Checks if given route between to stops isn't too long.
        Returns 'None' if route is deemed fine.
        Otherwise returns new 'status'.
        """
        total_route_dist = total_length(route)
        total_straight_dist = total_length(straight_route)
        dist_ratio = total_route_dist / total_straight_dist if total_straight_dist else 1

        expected_std_ratio = 7 if from_stop[:4] == to_stop[:4] else 3.5
        expected_ovr_ratio = self.override_ratios.get((from_stop, to_stop))
        expected_ratio = expected_ovr_ratio or expected_std_ratio

        too_long = dist_ratio > expected_ratio

        # Additional check if expected ovr_ratio is neccessary
        if expected_ovr_ratio and dist_ratio <= expected_std_ratio:
            self.logger.warn(
                f"Unnecessary pair ({from_stop}, {to_stop}) in OVERRIDE_SHAPE_RATIOS "
                f"(actual dist. ratio: {dist_ratio:.2f})"
            )

        if too_long:
            return f"route_too_long_{dist_ratio:.2f}_expected_{expected_ratio:.2f}"

    def route_between_stops(self, from_stop: str, to_stop: str, transport: str) \
            -> List[Tuple[float, float, float]]:
        """
        Tries to find route from one stop_id to other stop_id.
        Returns a list of [lat, lon, dist_from_start].
        """
        # Get the router
        router = self._router(transport)

        # Get start and end node ids
        start_node = self.get_node(from_stop, transport)
        end_node = self.get_node(to_stop, transport)

        # Check if a 'via' point is required
        if (via_point := self.force_via.get((from_stop, to_stop))):
            via_node = self._kdtree(transport).search_nn(via_point).id
        else:
            via_node = None

        # Do the route
        try:
            status, route = self.do_route(router, start_node, end_node, via_node)
        except TimeoutError:
            status, route = "timeout", []

        # Convert route to list of (lat, lon) pairs
        route = [router.nodeLatLon(i) for i in route]
        straight_route = self.staright_line(from_stop, to_stop)

        # Ensure `route` has at least one node
        if len(route) <= 1:
            route = straight_route

        # Calculate the ratio between route length and staright line between start and end stop
        if (too_long_msg := self._calculate_ratio(from_stop, to_stop, route, straight_route)):
            status = too_long_msg

        # Simplify route using the RDP formula
        if self.simplify:
            route = simplify_line(route, 0.000006)

        # Dump shape generation errors
        if status != "success":
            if self.dump_shape_issues:
                self._dump_shape_err(from_stop, to_stop, start_node, end_node, route, status)
            route = straight_route

        # Tranform route from (lat, lon) to (lat, lon, dist_from_start)
        route_with_dist: List[Tuple[float, float, float]] = []
        while route:
            point = route.pop(0)

            if not route_with_dist:
                route_with_dist.append((*point, 0.0))
            else:
                prev_point = route_with_dist[-1]
                route_with_dist.append(
                    (*point, prev_point[2] + distHaversine(prev_point[:2], point))  # type: ignore
                )

        return route_with_dist

    # Generating route for a pattern

    def get(self, route_type: str, route_id: str, variant_id: str, stops: Sequence[str]) \
            -> Tuple[str, Mapping[int, float]]:
        """
        Generates shape for a specific variant.
        Returns (shape_id, {stop_sequence: shape_dit_traveled}).
        """
        # Check if route's variant was already saved
        shape_id = route_id + "/" + variant_id
        distances = self.written_shapes.get(shape_id)

        if distances is not None:
            return shape_id, distances

        point_sequence = -1
        total_dist = 0.0
        distances = {0: 0.0}

        legs = (
            self.route_between_stops(stops[i-1], stops[i], route_type)
            for i in range(1, len(stops))
        )

        for stop_sequence, leg in enumerate(legs, 1):
            # The very first point of a leg is always the same as previous leg's last point,
            # So it's normally omitted. However, for the very first leg, there's no
            # »previous leg«
            if stop_sequence == 1:
                point_sequence += 1
                self.writer.writerow([
                    shape_id, point_sequence, "0.0", leg[0][0], leg[0][1]
                ])

            # Iterate over points of given leg
            for point in leg[1:]:
                point_sequence += 1
                point_dist = total_dist + point[2]

                self.writer.writerow([
                    shape_id, point_sequence, f"{point_dist:.4f}", point[0], point[1]
                ])

            # Save this leg distance
            total_dist += leg[-1][2]
            distances[stop_sequence] = total_dist

        # Save distances
        self.written_shapes[shape_id] = distances
        return shape_id, distances

    # File handling

    def open(self, target_dir: str, clear_shape_errs: bool = True) -> None:
        """Opens required files."""
        # Clear already-written shapes
        self.written_shapes = {}

        # Create file object
        file_path = os.path.join(target_dir, "shapes.txt")
        self.file_obj = open(file_path, "w", encoding="utf-8", newline="")

        # Create a csv.writer object
        self.writer = csv.writer(self.file_obj)
        self.writer.writerow(HEADERS["shapes.txt"])

        # Clean the DIR_SHAPES_ERR directory
        ensure_dir_exists(DIR_SHAPE_ERR, clear_shape_errs)

    def close(self) -> None:
        """Closes opened files."""
        self.file_obj.close()
