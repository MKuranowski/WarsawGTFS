from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from bs4 import BeautifulSoup
import pyroutelib3
import requests
import zipfile
import json
import csv
import rdp
import io
import os
import re

from .utils import haversine, iter_haversine, time_limit

"""
Random shit that is used by static data parser
"""

def proper_stop_names():
    """Get table with properly-cased stop names — ZTM data useally writes stop names in UPPER CASE"""
    names_table = {"4040": "Lotnisko Chopina", "1484": "Dom Samotnej Matki"}
    website = requests.get("http://m.ztm.waw.pl/rozklad_nowy.php?c=183&l=1")
    website.encoding = "utf8"
    soup = BeautifulSoup(website.text, "html.parser").find("div", id="RozkladContent")

    for t in soup.find_all("form"):
        t.decompose()

    for link in soup.find_all("a"):
        match = re.search(r"(?<=&a=)\d{4}", link.get("href"))
        if match:
            for t in link.find_all(True): t.decompose()
            name = link.string
            if name:
                name = name.replace(".", ". ").replace("-", " - ").replace("  "," ").rstrip()
                name = name.replace("Praga - Płd.", "Praga-Płd.")
                names_table[match.group(0)] = name

    return names_table

def route_color_type(number, desc):
    "Return route_type, route_color and route_text_color based on route number and description"
    desc = desc.lower()
    if "kolei" in desc: return "2", "000088", "FFFFFF"
    elif "tram" in desc: return "0", "B60000", "FFFFFF"
    elif "specjalna" in desc and number in {"W", "M"}: return "0", "B60000", "FFFFFF"
    elif "nocna" in desc: return "3", "000000", "FFFFFF"
    elif "strefowa" in desc: return "3", "006800", "FFFFFF"
    elif "ekspresowa" in desc or "przyspieszona" in desc: return "3", "B60000", "FFFFFF"
    else: return "3", "880077", "FFFFFF"

def normal_stop_name(name):
    return name.title().replace(".", ". ").replace("-", " - ").replace("  "," ").rstrip()

def normal_time(time):
    return ":".join(["0" + i if len(i) == 1 else i for i in (time.split(".") + ["00"])])

def should_town_be_added_to_name(stop_ref, stop_name, town_name, town_code):
    stop_name, town_name = map(str.upper, (stop_name, town_name))
    if town_code == "--": return False # Warsaw
    elif stop_ref[1:3] in {"90", "91", "92"}: return False # Rail Stops
    elif "PKP" in stop_name: return False
    elif town_name in stop_name: return False
    for town_part_name in town_name.split(" "):
        if town_part_name in stop_name:
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
            "205202-205203":  8.4, "434601-415002": 4.1, "396001-332101":  4.8, "102813-102811":    9,
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
            request = requests.get("https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm")

        else:
            # That's an overpass query for roads around Warsaw metro area
            #request = requests.get(r"https://overpass-api.de/api/interpreter/?data=%5Bbbox%3A51.92%2C20.46%2C52.49%2C21.465%5D%5Bout%3Axml%5D%3B%0A(%0A%20way%5B%22highway%22%3D%22motorway%22%5D%3B%0A%20way%5B%22highway%22%3D%22motorway_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22unclassified%22%5D%3B%0A%20way%5B%22highway%22%3D%22minor%22%5D%3B%0A%20way%5B%22highway%22%3D%22residential%22%5D%3B%0A%20way%5B%22highway%22%3D%22living_street%22%5D%3B%0A%20way%5B%22highway%22%3D%22service%22%5D%3B%0A)%3B%0A(._%3B%3E%3B)%3B%0Aout%3B")

            # And this one also contains turn restrictions
            request = requests.get(r"https://overpass-api.de/api/interpreter/?data=%5Bbbox%3A51.92%2C20.46%2C52.49%2C21.465%5D%5Bout%3Axml%5D%3B%0A(%0A%20way%5B%22highway%22%3D%22motorway%22%5D%3B%0A%20way%5B%22highway%22%3D%22motorway_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22unclassified%22%5D%3B%0A%20way%5B%22highway%22%3D%22minor%22%5D%3B%0A%20way%5B%22highway%22%3D%22residential%22%5D%3B%0A%20way%5B%22highway%22%3D%22living_street%22%5D%3B%0A%20way%5B%22highway%22%3D%22service%22%5D%3B%0A)%3B%0A%3E-%3E.n%3B%0A%3C-%3E.r%3B%0A(._%3B.n%3B.r%3B)%3B%0Aout%3B%0A")

        temp_xml = NamedTemporaryFile(delete=False)
        temp_xml.write(request.content)
        temp_xml.seek(0)

        router = pyroutelib3.Router(routing_type, temp_xml.name)

        temp_xml.close()

        return router

    def rotue_between_stops(self, start_stop, end_stop, route_type):
        # print("\033[1A\033[K" + "Getting shape between {} and {}".format(start_stop, end_stop)) # DEBUG
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
            else: raise ValuError("invalid type: {}".format(route_type))

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

                route_points = [[start_lat, start_lon], [end_lat, end_lon]]

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

    def close(self):
        self.file.close()
