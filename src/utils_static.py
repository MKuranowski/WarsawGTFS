from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from bs4 import BeautifulSoup
import pyroutelib3
import requests
import zipfile
import json
import math
import csv
import rdp
import io
import os
import re

from .utils import haversine, iter_haversine, time_limit

"""
Random shit that is used by static data parser
"""

def route_color_type(number, desc):
    "Return route_type, route_color and route_text_color based on route number and description"
    desc = desc.lower()
    if "kolei" in desc: return "2", "009955", "FFFFFF"
    elif "tram" in desc: return "0", "B60000", "FFFFFF"
    elif "specjalna" in desc and number in {"W", "M"}: return "0", "B60000", "FFFFFF"
    elif "nocna" in desc: return "3", "000000", "FFFFFF"
    elif "uzupełniająca" in desc: return "3", "000088", "FFFFFF"
    elif "strefowa" in desc: return "3", "006800", "FFFFFF"
    elif "ekspresowa" in desc or "przyspieszona" in desc: return "3", "B60000", "FFFFFF"
    else: return "3", "880077", "FFFFFF"

def normal_stop_name(name):
    # add .title() if ZTM provides names in ALL-UPPER CASE again
    name = name.replace(".", ". ")      \
               .replace("-", " - ")     \
               .replace("  ", " ")      \
               .replace("al.", "Al.")   \
               .replace("pl.", "Pl.")   \
               .replace("os.", "Os.")   \
               .replace("ks.", "Ks.")   \
               .replace("Ak ", "AK ")   \
               .replace("Ch ", "CH ")   \
               .replace("gen.", "Gen.") \
               .replace("rondo ", "Rondo ") \
               .rstrip()

    return name

def normal_time(time, lessthen24=False):
    h, m = map(int, time.split("."))
    if lessthen24:
        while h >= 24: h -= 24
    return f"{h:0>2}:{m:0>2}:00"

def should_town_be_added_to_name(group):
    group_name = group["name"].upper(),
    group_town = group["town"].upper()
    group_code = group["town_code"].upper()

    if group_code == "--": return False # Warsaw
    elif group["id"][1:3] in {"90", "91", "92"}: return False # Rail Stops
    elif "PKP" in group_name: return False
    elif group_town in group_name: return False
    for town_part_name in group_town.split(" "):
        if town_part_name in group_name:
            return False
    return True

def proper_headsign(stop_id, stop_name):
    "Get trip_headsign based on last stop_id and its stop_name"
    if stop_id in ["503803", "503804"]: return "Zjazd do zajezdni Wola"
    elif stop_id == "103002": return "Zjazd do zajezdni Praga"
    elif stop_id == "324010": return "Zjazd do zajezdni Mokotów"
    elif stop_id in ["606107", "606108"]: return "Zjazd do zajezdni Żoliborz"
    elif stop_id.startswith("4202"): return "Lotnisko Chopina"
    else: return stop_name

def trip_direction(trip_original_stops, direction_stops):
    """
    Guess the trip direction_id based on trip_original_stops, and
    a direction_stops which should be a dictionary with 2 keys: "0" and "1" -
    corresponding values should be sets of stops encountered in given dir
    """
    # Stops for each direction have to be unique
    dir_stops_0 = direction_stops["0"].difference(direction_stops["1"])
    dir_stops_1 = direction_stops["1"].difference(direction_stops["0"])

    # Trip stops in direction 0 and direction 1
    trip_stops_0 = trip_original_stops.intersection(dir_stops_0)
    trip_stops_1 = trip_original_stops.intersection(dir_stops_1)

    # Amount of stops of trip in each direction
    trip_stops_0_len = len(trip_stops_0)
    trip_stops_1_len = len(trip_stops_1)

    # More or equal stops belonging to dir_0 then dir_1 => "0"
    if trip_stops_0_len >= trip_stops_1_len:
        return "0"

    # More stops belonging to dir_1
    elif trip_stops_0_len < trip_stops_1_len:
        return "1"

    # How did we get here
    else:
        raise RuntimeError(f"{trip_stops_0_len} is not bigger, equal or less then {trip_stops_1_len}")

class Metro:
    @staticmethod
    def _FieldNames(f):
        r = csv.DictReader(f)
        return r.fieldnames

    @classmethod
    def _RewriteCalendar(self, filename, metro_file):
        gtfs_fileloc = os.path.join("gtfs", filename)
        dates_ztm = set()
        dates_metro = set()

        calendars = {}

        # Load ZTM Calendars
        with open(gtfs_fileloc, "r", encoding="utf-8", newline="") as f:
            gtfs_reader = csv.DictReader(f)
            gtfs_fieldnames = gtfs_reader.fieldnames
            for row in gtfs_reader:
                dates_ztm.add(datetime.strptime(row["date"], "%Y%m%d").date())
                if row["date"] not in calendars: calendars[row["date"]] = []
                calendars[row["date"]].append(row["service_id"])

        # Load Metro Calendars
        metro_buffer = io.TextIOWrapper(metro_file, encoding="utf-8", newline="")
        metro_reader = csv.DictReader(metro_buffer)
        for row in metro_reader:
            dates_metro.add(datetime.strptime(row["date"], "%Y%m%d").date())
            if row["date"] not in calendars: calendars[row["date"]] = []
            calendars[row["date"]].append(row["service_id"])

        # Find date range
        start_date = max(min(dates_ztm), min(dates_metro))
        end_date = min(max(dates_ztm), max(dates_metro))

        # Create new file
        with open(gtfs_fileloc, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["date", "service_id", "exception_type"])
            writer.writeheader()
            while start_date <= end_date:
                date_str = start_date.strftime("%Y%m%d")
                for service in calendars[date_str]:
                    writer.writerow({"date": date_str, "service_id": service, "exception_type": "1"})
                start_date += timedelta(1)

    @classmethod
    def _RewriteFile(self, filename, metro_file):
        gtfs_fileloc = os.path.join("gtfs", filename)

        if os.path.exists(gtfs_fileloc):
            # Get gtfs file header
            with open(gtfs_fileloc, "r", encoding="utf-8", newline="") as f:
                gtfs_fieldnames = self._FieldNames(f)

            # Decode metrofile
            metro_buffer = io.TextIOWrapper(metro_file, encoding="utf-8", newline="")
            metro_reader = csv.DictReader(metro_buffer)

            # Append to gtfs - csv module is to keep columns aligned
            with open(gtfs_fileloc, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=gtfs_fieldnames, extrasaction="ignore")
                for row in metro_reader:
                    if filename == "trips.txt" and not row.get("exceptional", ""): row["exceptional"] = "0"
                    if filename == "routes.txt": row["agency_id"] = "0"
                    writer.writerow(row)

        else:
            # If file does not exist then simply copy it, without caring about the content
            with open(gtfs_fileloc, "a", encoding="utf-8", newline="\r\n") as f:
                for row in metro_file:
                    row = str(row, "utf-8")
                    f.write(row.rstrip() + "\n")

    @classmethod
    def add(self):
        feed = requests.get("https://mkuran.pl/feed/metro/metro-latest.zip")
        buffer = io.BytesIO(feed.content)
        archive = zipfile.ZipFile(buffer)
        files = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt", \
                 "calendar_dates.txt", "frequencies.txt", "shapes.txt"]
        for filename in files:
            with archive.open(filename) as metrofile:
                if filename == "calendar_dates.txt": self._RewriteCalendar(filename, metrofile)
                else: self._RewriteFile(filename, metrofile)
        archive.close()

class Shaper:
    def __init__(self, bus_router=None, train_router=None, tram_router=None):
        self.stops = {}
        self.trips = {}
        self.osm_stops = {}
        self.failed = set()

        self.ratios = {
            "103102-103101":    9, "103103-103101":   9, "207902-201801":  3.5, "700609-700614": 18.5, "102805-102811":  8.1,
            "102810-102811": 12.5, "410201-419902": 3.7, "600516-607505":  3.6, "120502-120501": 15.5, "607506-607501": 14.3,
            "600517-607505":  3.8, "205202-205204": 7.6, "100610-100609": 18.4, "201802-226002":  3.8, "325402-325401":   24,
            "400901-400806":  5.5, "600515-607505":   4, "600513-607505":  4.4, "124001-124003":   11, "124202-124201": 13.3,
            "105004-115402":  5.5, "243801-203903": 5.2, "301201-301202":  8.3, "600514-607505":    4, "424502-405952": 11.8,
            "124001-124003": 17.5, "124202-143702": 3.8, "102502-102504": 13.6, "703301-703302": 11.1, "401505-401560":   10,
            "100610-100609":   19, "102502-102504":  15, "102805-102811":  8.5, "206101-206102": 12.4, "415001-405902":  4.1,
            "205202-205203":  8.4, "434601-415002": 4.1, "396001-332101":  4.8, "102813-102811":    9, "403601-403602": 10.5,
            "406404-406401":  7.1, "600513-607505": 4.6, "600515-607505":  4.6, "700214-700211":  7.1, "707602-707603": 10.5,
        }

        self._load_stops()

        if bus_router == None:
            self.bus_router = self.create_router("bus")

        if train_router == None:
            self.train_router = self.create_router("train")

        if tram_router == None:
            self.tram_router = self.create_router("tram")

    def _load_stops(self):
        overpass_query = requests.get("https://overpass-api.de/api/interpreter?data=%5Bbbox%3A51.921819%2C20.462668%2C52.48293%2C21.46385%5D%5Bout%3Ajson%5D%3Bnode%5Bpublic_transport%3Dstop_position%5D%5Bnetwork%3D%22ZTM%20Warszawa%22%5D%3Bout%3B").json()
        for i in overpass_query["elements"]:
            try:
                self.osm_stops[str(i["tags"]["ref"])] = i["id"]
            except KeyError:
                continue

    @staticmethod
    def create_router(transport):
        print("\033[1A\033[K" + "Creating shaper for {}".format(transport))
        if transport == "bus":
            routing_type = {
                "weights": {"motorway": 1.5, "trunk": 1.5, "primary": 1.4, "secondary": 1.3, "tertiary": 1.3,
                "unclassified": 1, "residential": 0.6, "living_street": 0.6, "track": 0.3, "service": 0.5},
                "access": ["access", "vehicle", "motor_vehicle", "psv", "bus", "routing:ztm"],
                "name": "bus"
            }
        elif transport == "train":
            routing_type = "train"
        elif transport == "tram":
            routing_type = "tram"
        else:
            raise ValueError("Invalid transport type {} for Shaper".format(transport))

        if transport in {"train", "tram"}:
            request = requests.get("https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm", stream=True)

        else:
            # Overpass Query:
            query = "\n".join([
                "[bbox:51.9144,20.4438,52.5007,21.4844][out:xml];"
                "("
                ' way["highway"="motorway"];'
                ' way["highway"="motorway_link"];'
                ' way["highway"="trunk"];'
                ' way["highway"="trunk_link"];'
                ' way["highway"="primary"];'
                ' way["highway"="primary_link"];'
                ' way["highway"="secondary"];'
                ' way["highway"="secondary_link"];'
                ' way["highway"="tertiary"];'
                ' way["highway"="tertiary_link"];'
                ' way["highway"="unclassified"];'
                ' way["highway"="minor"];'
                ' way["highway"="residential"];'
                ' way["highway"="living_street"];'
                ' way["highway"="service"];'
                ');'
                'way._(poly:"52.4455 20.6858 52.376 20.6872 52.3533 20.7868 52.2929 20.726 52.2694 20.6724 52.2740 20.4465 52.2599 20.4438 52.2481 20.5832 52.2538 20.681 52.1865 20.6786 52.1859 20.7129 52.1465 20.7895 52.0966 20.783 52.0632 20.7222 52.0151 20.7617 51.9873 20.9351 51.9269 20.9509 51.9144 21.0226 51.9322 21.1987 51.9569 21.2472 52.0463 21.2368 52.1316 21.4844 52.1429 21.4404 52.2130 21.3814 52.2622 21.3141 52.2652 21.1977 52.3038 21.173 52.3063 21.2925 52.3659 21.3515 52.3829 21.3001 52.4221 21.1929 52.4898 21.1421");'
                '>->.n;'
                '<->.r;'
                '(._;.n;.r;);'
                'out;'
            ])

            request = requests.get(
                "https://overpass-api.de/api/interpreter/",
                params={"data": query},
                stream=True,
            )

        temp_xml = NamedTemporaryFile(delete=False)

        for chunk in request.iter_content(chunk_size=1024):
            temp_xml.write(chunk)

        request.close()
        temp_xml.seek(0)

        router = pyroutelib3.Router(routing_type, temp_xml.name)

        temp_xml.close()

        return router

    def rotue_between_stops(self, start_stop, end_stop, route_type):
        #print("\033[1A\033[K" + "Getting shape between {} and {}".format(start_stop, end_stop)) # DEBUG
        # Find nodes
        start_lat, start_lon = map(float, self.stops[start_stop])
        end_lat, end_lon = map(float, self.stops[end_stop])

        # Start node
        if route_type == "3" and self.osm_stops.get(start_stop, None) in self.bus_router.rnodes:
            start = self.osm_stops[start_stop]

        else:
            if route_type == "3": start = self.bus_router.findNode(start_lat, start_lon)
            elif route_type == "2": start = self.train_router.findNode(start_lat, start_lon)
            elif route_type == "0": start = self.tram_router.findNode(start_lat, start_lon)
            else: raise ValueError("invalid type: {}".format(route_type))

        # End node
        if route_type == "3" and self.osm_stops.get(end_stop, None) in self.bus_router.rnodes:
            end = self.osm_stops[end_stop]

        else:
            if route_type == "3": end = self.bus_router.findNode(end_lat, end_lon)
            elif route_type == "2": end = self.train_router.findNode(end_lat, end_lon)
            elif route_type == "0": end = self.tram_router.findNode(end_lat, end_lon)
            else: raise ValuError("invalid type: {}".format(route_type))

        # Do route

        # SafetyCheck - start and end nodes have to be defined
        if start and end:
            try:
                with time_limit(10):
                    if route_type == "3": status, route = self.bus_router.doRoute(start, end)
                    elif route_type == "2": status, route = self.train_router.doRoute(start, end)
                    elif route_type == "0": status, route = self.tram_router.doRoute(start, end)
                    else: raise ValuError("invalid type: {}".format(route_type))
            except TimeoutError:
                status, route = "timeout", []

            if route_type == "3": route_points = list(map(self.bus_router.nodeLatLon, route))
            elif route_type == "2": route_points = list(map(self.train_router.nodeLatLon, route))
            elif route_type == "0": route_points = list(map(self.tram_router.nodeLatLon, route))
            else: raise ValuError("invalid type: {}".format(route_type))

            try: dist_ratio = iter_haversine(route_points) / haversine([start_lat, start_lon], [end_lat, end_lon])
            except ZeroDivisionError: dist_ratio = 1

            # SafetyCheck - route has to have at least 2 nodes
            if status == "success" and len(route_points) <= 1:
                status = "to_few_nodes_({d})".format(len(route))

            # SafetyCheck - route can't be unbelivabely long than straight line between stops
            # Except for stops in same stop group
            elif start_stop[:4] == end_stop[:4] and dist_ratio > self.ratios.get(start_stop + "-" + end_stop, 7):
                status = "route_too_long_in_group_ratio:{:.2f}".format(dist_ratio)

            elif start_stop[:4] != end_stop[:4] and dist_ratio > self.ratios.get(start_stop + "-" + end_stop, 3.5):
                status = "route_too_long_ratio:{:.2f}".format(dist_ratio)

            # Apply rdp algorithm
            route_points = rdp.rdp(route_points, epsilon=0.000006)

        else:
            start, end = math.nan, math.nan
            dist_ratio = math.nan
            status = "no_nodes_found"

        # If we failed, catch some more info on why
        if status != "success":

            ### DEBUG-SHAPES ###
            if status.startswith("route_too_long"):
                if not os.path.exists("shape-errors/{}-{}.json".format(start_stop, end_stop)):
                    with open("shape-errors/{}-{}.json".format(start_stop, end_stop), "w") as f:
                        json.dump(
                            {"type": "FeatureCollection","features": [{
                             "type": "Feature", "properties": {"ratio": dist_ratio, "stops": "{}-{}".format(start_stop, end_stop)},
                             "geometry": {"type":"LineString", "coordinates": [[i[1], i[0]] for i in route_points]}
                            }]}, f, indent=2
                        )

            else:
                if not os.path.exists("shape-errors/{}-{}.json".format(start_stop, end_stop)):
                    with open("shape-errors/{}-{}.json".format(start_stop, end_stop), "w") as f:
                        json.dump(
                            {"start": start_stop, "end": end_stop,
                             "start_node": start, "end_node": end,
                             "error": status
                            }, f, indent=2
                        )

            route_points = [[start_lat, start_lon], [end_lat, end_lon]]

        # Add distances to route points
        dist = 0.0
        for x in range(len(route_points)):
            if x == 0:
                route_points[x].append(dist)
            else:
                dist += haversine((route_points[x-1][0], route_points[x-1][1]), (route_points[x][0], route_points[x][1]))
                route_points[x].append(dist)

        return route_points

    def get(self, route_type, trip_id, stops):
        pattern_id = trip_id.split("/")[0] + "/" + trip_id.split("/")[1]

        if pattern_id in self.trips:
            return pattern_id, self.trips[pattern_id]

        pt_seq = 0
        dist = 0.0
        distances = {0: 0.0}

        routes = []
        for i in range(1, len(stops)):
            routes.append(self.rotue_between_stops(stops[i-1], stops[i], route_type))

        for x in range(len(routes)):
            leg = routes[x]

            # We always ignore first point of route leg [it's the same as next routes first point],
            # but this would make the script ignore the very first point (corresponding to start stop)
            if x == 0:
                pt_seq += 1
                self.writer.writerow([pattern_id, pt_seq, round(dist, 4), leg[0][0], leg[0][1]])

            # Output points of leg
            for y in range(1, len(leg)):
                pt_seq += 1
                self.writer.writerow([pattern_id, pt_seq, round(dist + leg[y][2], 4), leg[y][0], leg[y][1]])

            # Add generic info about leg
            dist += leg[-1][2]
            distances[x+1] = dist

        self.trips[pattern_id] = distances
        return pattern_id, distances

    def open(self):
        self.file = open("gtfs/shapes.txt", "w", encoding="utf-8", newline="")
        self.writer = csv.writer(self.file)
        self.writer.writerow(["shape_id", "shape_pt_sequence", "shape_dist_traveled", "shape_pt_lat", "shape_pt_lon"])

        self.stops = {}
        self.trips = {}
        self.failed = set()

    def close(self):
        self.file.close()
