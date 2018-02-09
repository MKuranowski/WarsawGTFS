from math import radians, cos, sin, asin, sqrt
from tempfile import NamedTemporaryFile
from pyroutelib3 import Router, TYPES
from contextlib import contextmanager
from warnings import warn
from copy import copy
from rdp import rdp
import overpass
import requests
import signal

_RDP_EPSILON = 0.000006
_RAIL_FILE = "https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm"
_TRAM_FILE = "https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm"
_BUS_FILE = "https://overpass-api.de/api/interpreter?data=%5Bbbox%3A51%2E921819%2C20%2E462668%2C52%2E48293%2C21%2E46385%5D%5Bout%3Axml%5D%3B%28way%5B%22highway%22%3D%22motorway%22%5D%3Bway%5B%22highway%22%3D%22motorway%5Flink%22%5D%3Bway%5B%22highway%22%3D%22trunk%22%5D%3Bway%5B%22highway%22%3D%22trunk%5Flink%22%5D%3Bway%5B%22highway%22%3D%22primary%22%5D%3Bway%5B%22highway%22%3D%22primary%5Flink%22%5D%3Bway%5B%22highway%22%3D%22secondary%22%5D%3Bway%5B%22highway%22%3D%22secondary%5Flink%22%5D%3Bway%5B%22highway%22%3D%22tertiary%22%5D%3Bway%5B%22highway%22%3D%22tertiary%5Flink%22%5D%3Bway%5B%22highway%22%3D%22motorway%22%5D%3Bway%5B%22highway%22%3D%22unclassified%22%5D%3Bway%5B%22highway%22%3D%22minor%22%5D%3Bway%5B%22highway%22%3D%22residential%22%5D%3Bway%5B%22highway%22%3D%22service%22%5D%3B%29%3B%28%2E%5F%3B%3E%3B%29%3Bout%3B%0A"
_OVERRIDE_RATIO = {"103102-103101": 9, "103103-103101": 9, "207902-201801": 3.5, "700609-700614": 18.5, "102805-102811": 8.1, "205202-205203": 8.4,
                   "102810-102811": 12.5, "410201-419902": 3.7, "600516-607505": 3.6, "120502-120501": 15.5, "607506-607501": 14.3,
                   "600517-607505": 3.8, "205202-205204": 7.6, "100610-100609": 18.4, "201802-226002": 3.8, "325402-325401": 21.9,
                   "400901-400806": 5.5, "600515-607505": 4, "600513-607505": 4.4,"124001-124003": 11, "124202-124201": 13.3, "102813-102811": 9,
                   "105004-115402": 5.5}

TYPES["bus"] = {
        "weights": {"motorway": 1.5, "trunk": 1.5, "primary": 1.4, "secondary": 1.3, "tertiary": 1.3,
            "unclassified": 1, "residential": 0.6, "track": 0.3, "service": 0.5},
        "access": ["access", "vehicle", "motor_vehicle", "psv", "bus", "routing:ztm"]}

class Timeout(Exception):
    pass

@contextmanager
def limit_time(sec):
    "Time limter based on https://gist.github.com/Rabbit52/7449101"
    def handler(x, y): raise Timeout
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(sec)
    try: yield
    finally: signal.alarm(0)

def _distance(pt1, pt2):
    "Calculate havresine distance"
    lat1, lon1 = map(radians, pt1)
    lat2, lon2 = map(radians, pt2)
    lat = lat2 - lat1
    lon = lon2 - lon1
    d = sin(lat * 0.5) ** 2 + cos(lat1) * cos(lat2) * sin(lon * 0.5) ** 2
    return 2 * 6371 * asin(sqrt(d))

def _totalDistance(points):
    "Calculate total route distance"
    total = 0.0
    for i in range(1, len(points)):
        total += _distance(points[i-1], points[i])
    return total

class Shaper(object):
    def __init__(self, enabled):
        self.enabled = enabled
        self.api = overpass.API()
        self.router = None
        self.transport = None
        self.stops = {}
        self.trips = {}
        self.osmStops = {}
        self.failed = {}
        self.file = open("output/shapes.txt", "w", encoding="utf-8", newline="\r\n")
        self.file.write("shape_id,shape_pt_sequence,shape_dist_traveled,shape_pt_lat,shape_pt_lon\n")

        self._loadStops()

    def _loadStops(self):
        features = self.api.Get("node[public_transport=stop_position][network=\"ZTM Warszawa\"]")["features"]
        for i in features:
            try:
                self.osmStops[str(i["properties"]["ref"])] = i["id"]
            except KeyError:
                continue

    def nextRoute(self, short_name, transport):
        self.trips.clear()

        if transport == "0": transport = "tram"
        elif transport == "3": transport = "bus"
        elif transport == "2": transport = "train"
        else: raise ValueError("Invalid transport type {} for Shaper".format(transport))

        if not self.enabled:
            self.router = None


        elif short_name == "WKD":
            warn("Shape creation is not available for WKD line")
            self.router = None

        elif transport != self.transport:
            temp_xml = NamedTemporaryFile(delete=False)
            if transport == "train":
                request = requests.get(_RAIL_FILE)

            elif transport == "tram":
                request = requests.get(_TRAM_FILE)

            else:
                request = requests.get(_BUS_FILE)

            temp_xml.write(request.content)
            self.router = Router(transport, temp_xml.name)
            temp_xml.close()

        self.transport = transport

    def get(self, trip_id, stops):
        pattern_id = trip_id.split("/")[0] + "/" + trip_id.split("/")[1]

        if pattern_id in self.trips:
            return self.trips[pattern_id]

        elif not self.router:
            return None

        pt_seq = 0
        dist = 0.0
        distances = {}

        for x in range(1, len(stops)):
            # Find nodes
            start_stop, end_stop = stops[x-1], stops[x]
            start_lat, start_lon = map(float, self.stops[start_stop])
            end_lat, end_lon = map(float, self.stops[end_stop])

            try:
                assert self.transport in ["tram", "bus"]
                start = self.osmStops[start_stop]
                assert start in self.router.data.rnodes
            except (AssertionError, KeyError):
                start = self.router.data.findNode(start_lat, start_lon)

            try:
                assert self.transport in ["tram", "bus"]
                end = self.osmStops[end_stop]
                assert end in self.router.data.rnodes
            except (AssertionError, KeyError):
                end = self.router.data.findNode(end_lat, end_lon)

            # Do route
            # SafetyCheck - start and end nodes have to be defined
            if start and end:
                try:
                    with limit_time(10):
                        status, route = self.router.doRoute(start, end)
                except Timeout:
                    status, route = "timeout", []

                route_points = list(map(self.router.nodeLatLon, route))

                dist_ratio = _totalDistance(route_points) / _distance([start_lat, start_lon], [end_lat, end_lon])

                # SafetyCheck - route has to have at least 2 nodes
                if status == "success" and len(route_points) <= 1:
                    status = "to_few_nodes_(%d)" % len(route)

                # SafetyCheck - route can't be unbelivabely long than straight line between stops
                # Except for stops in same stop group
                elif stops[x-1][:4] == stops[x][:4] and dist_ratio > _OVERRIDE_RATIO.get(start_stop + "-" + end_stop, 7):
                    status = "route_too_long_in_group_ratio:%s" % round(dist_ratio, 2)

                elif stops[x-1][:4] != stops[x][:4] and dist_ratio > _OVERRIDE_RATIO.get(start_stop + "-" + end_stop, 3.5):
                    status = "route_too_long_ratio:%s" % round(dist_ratio, 2)

                # Apply rdp algorithm
                route_points = rdp(route_points, epsilon=_RDP_EPSILON)

            else:
                start, end = "n/d", "n/d"
                status = "no_nodes_found"

            if status != "success":
                route_points = [[start_lat, start_lon], [end_lat, end_lon]]
                if self.failed.get(start_stop + "-" + end_stop, True):
                    self.failed[start_stop + "-" + end_stop] = False
                    print("Shaper: Error between stops '%s' (%s) - '%s' (%s): %s " % (start_stop, start, end_stop, end, status))

            if x == 1:
                # See below, except when it's the very first stop of a trip
                distances[1] = str(dist)
                self.file.write(",".join([pattern_id, str(pt_seq), str(dist), str(route_points[0][0]),  str(route_points[0][1])]) + "\n")

            for y in range(1, len(route_points)):
                # Don't write the first point, as it is the same as previous stop pair last point
                pt_seq += 1
                dist += _distance(route_points[y-1], route_points[y])
                self.file.write(",".join([pattern_id, str(pt_seq), str(dist), str(route_points[y][0]), str(route_points[y][1])]) + "\n")

            distances[x + 1] = str(dist)

        self.trips[pattern_id] = distances
        return distances
