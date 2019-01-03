import io
import os
import re
import csv
import json
import time
import math
import signal
import py7zlib
import zipfile
import argparse
import requests
import pyroutelib3
from rdp import rdp
from ftplib import FTP
from warnings import warn
from bs4 import BeautifulSoup
from collections import OrderedDict
from datetime import datetime, date, timedelta
from contextlib import contextmanager
from tempfile import TemporaryFile, NamedTemporaryFile

# Update bus preferences in pyroutelib3
pyroutelib3.TYPES["bus"] = {
        "weights": {"motorway": 1.5, "trunk": 1.5, "primary": 1.4, "secondary": 1.3, "tertiary": 1.3,
            "unclassified": 1, "residential": 0.6, "living_street": 0.6, "track": 0.3, "service": 0.5},
        "access": ["access", "vehicle", "motor_vehicle", "psv", "bus", "routing:ztm"],
        "name": "bus"
}

# Other functions used in data parsing

def _ProperStopNames():
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

def _NormalizeName(name):
    return name.title().replace(".", ". ").replace("-", " - ").replace("  "," ").rstrip()

def _NormalizeTime(time):
    return ":".join(["0" + i if len(i) == 1 else i for i in (time.split(".") + ["00"])])

def _AddTownToStopName(stop_ref, stop_name, town_name, town_code):
    stop_name, town_name = map(str.upper, (stop_name, town_name))
    if town_code == "--": return False # Warsaw
    elif stop_ref[1:3] in {"90", "91", "92"}: return False # Rail Stops
    elif "PKP" in stop_name: return False
    elif town_name in stop_name: return False
    for town_part_name in town_name.split(" "):
        if town_part_name in stop_name:
            return False
    return True

def _GroupAvgPosition(stops_in_group):
    lats = list(map(float, [i[0] for i in stops_in_group.values()]))
    lons = list(map(float, [i[1] for i in stops_in_group.values()]))
    avg_lat = round(sum(lats)/len(lats), 8)
    avg_lon = round(sum(lons)/len(lons), 8)
    return str(avg_lat), str(avg_lon)

def _RouteColorType(number, desc):
    "Return route_type, route_color and route_text_color based on route number and description"
    desc = desc.lower()
    if "kolei" in desc: return "2", "000088", "FFFFFF"
    elif "tram" in desc: return "0", "B60000", "FFFFFF"
    elif "specjalna" in desc and number in {"W", "M"}: return "0", "B60000", "FFFFFF"
    elif "nocna" in desc: return "3", "000000", "FFFFFF"
    elif "strefowa" in desc: return "3", "006800", "FFFFFF"
    elif "ekspresowa" in desc or "przyspieszona" in desc: return "3", "B60000", "FFFFFF"
    else: return "3", "880077", "FFFFFF"

def _Headsign(stop_id, stop_name):
    "Get trip_headsign based on last stop_id and its stop_name"
    if stop_id in ["503803", "503804"]: return "Zjazd do zajezdni Wola"
    elif stop_id == "103002": return "Zjazd do zajezdni Praga"
    elif stop_id == "324010": return "Zjazd do zajezdni Mokotów"
    elif stop_id in ["606107", "606108"]: return "Zjazd do zajezdni Żoliborz"
    elif stop_id.startswith("4202"): return "Lotnisko Chopina"
    else: return stop_name

def _Distance(pt1, pt2):
    "Calculate havresine distance"
    lat1, lon1 = map(math.radians, pt1)
    lat2, lon2 = map(math.radians, pt2)
    lat = lat2 - lat1
    lon = lon2 - lon1
    d = math.sin(lat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(lon * 0.5) ** 2
    return 2 * 6371 * math.asin(math.sqrt(d))

def _TotalDistance(points):
    "Calculate total route distance"
    total = 0.0
    for i in range(1, len(points)):
        total += _Distance(points[i-1], points[i])
    return total

def ClearDir(shapes=False):
    if not os.path.exists("gtfs"): os.mkdir("gtfs")
    for file in [os.path.join("gtfs", x) for x in os.listdir("gtfs")]: os.remove(file)
    if shapes:
        if not os.path.exists("shape-errors"): os.mkdir("shape-errors")
        for file in [os.path.join("shape-errors", x) for x in os.listdir("shape-errors")]: os.remove(file)

def StaticFiles(version, shapes):
    "Create files that don't depend of ZTM file content"
    file = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="\r\n")
    file.write('agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n')
    file.write('0,"Warszawski Transport Publiczny (ZTM Warszawa)","http://ztm.waw.pl",Europe/Warsaw,pl,19 115,"http://www.ztm.waw.pl/?c=110&l=1"\n')
    file.close()

    file = open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="\r\n")
    file.write('feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n')
    if shapes: file.write('"GTFS Convert: MKuranowski; Data: ZTM Warszawa; Bus Shapes (under ODbL License): © OpenStreetMap Contributors","https://github.com/MKuranowski/WarsawGTFS",pl,{}\n'.format(version))
    else: file.write('"GTFS Convert: MKuranowski; Data: ZTM Warszawa","https://github.com/MKuranowski/WarsawGTFS",pl,{}\n'.format(version))
    file.close()

def Compress():
    "Compress all created files to gtfs.zip"
    archive = zipfile.ZipFile("gtfs.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
    for file in os.listdir("gtfs"):
        if file.endswith(".txt"):
            archive.write(os.path.join("gtfs", file), arcname=file)
    archive.close()

def Download(requested_date=None):
    "Download and decompress schedules for current data. Returns tuple (TemporaryFile, version) - and that TemporaryFile is decompressed .TXT file"
    # Login to ZTM server and get the list of files
    server = FTP("rozklady.ztm.waw.pl")
    server.login()
    files = server.nlst()

    # If user has requested an exact version, check if it's on the server
    if requested_date:
        fname = "RA{}.7z".format(date)
        if fname not in files:
            raise KeyError("Requested file version ({}) not found on ZTM server".format(date))

    # If not, find one valid today
    else:
        fdate = date.today()
        while True:
            fname = fdate.strftime("RA%y%m%d.7z")
            if fname in files: break
            else: fdate -= timedelta(days=1)

    # Create temporary files for storing th 7z archive and the compressed TXT file
    temp_arch = TemporaryFile(mode="w+b")
    temp_text = TemporaryFile(mode="w+t")

    # Download the file
    server.retrbinary("RETR " + fname, temp_arch.write)
    server.quit()

    # Decompress the file and close the archive
    temp_arch.seek(0)
    arch = py7zlib.Archive7z(temp_arch)
    arch_file = arch.filenames[0]
    temp_text.write(str(arch.getmember(arch_file).read(), "cp1250"))
    temp_text.seek(0)
    temp_arch.close()

    version = re.match(r"RA\d{6}", arch_file.upper())[0]
    return temp_text, version

@contextmanager
def TimeLimit(sec):
    "Time limter based on https://gist.github.com/Rabbit52/7449101"
    def handler(x, y): raise TimeoutError
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(sec)
    try: yield
    finally: signal.alarm(0)

class Metro:
    @classmethod
    def _FieldNames(self, f):
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
    def __init__(self):
        self.router = None
        self.transport = None
        self.stops = {}
        self.trips = {}
        self.osmStops = {}
        self.failed = set()

        self.file = open("gtfs/shapes.txt", "w", encoding="utf-8", newline="")
        self.writer = csv.writer(self.file)
        self.writer.writerow(["shape_id", "shape_pt_sequence", "shape_dist_traveled", "shape_pt_lat", "shape_pt_lon"])

        self.ratios = {
            "103102-103101": 9, "103103-103101": 9, "207902-201801": 3.5, "700609-700614": 18.5, "102805-102811": 8.1, "205202-205203": 8.4,
            "102810-102811": 12.5, "410201-419902": 3.7, "600516-607505": 3.6, "120502-120501": 15.5, "607506-607501": 14.3,
            "600517-607505": 3.8, "205202-205204": 7.6, "100610-100609": 18.4, "201802-226002": 3.8, "325402-325401": 24,
            "400901-400806": 5.5, "600515-607505": 4, "600513-607505": 4.4,"124001-124003": 11, "124202-124201": 13.3, "102813-102811": 9,
            "105004-115402": 5.5, "243801-203903": 5.2, "301201-301202": 8.3, "600514-607505": 4, "424502-405952": 11.8, "396001-332101": 4.8,
            "124001-124003": 17.5, "124202-143702": 3.8, "102502-102504": 13.6, "703301-703302": 11.1, "401505-401560": 10,
        }

        self._loadStops()

    def _loadStops(self):
        overpass_query = requests.get("https://overpass-api.de/api/interpreter?data=%5Bbbox%3A51.921819%2C20.462668%2C52.48293%2C21.46385%5D%5Bout%3Ajson%5D%3Bnode%5Bpublic_transport%3Dstop_position%5D%5Bnetwork%3D%22ZTM%20Warszawa%22%5D%3Bout%3B").json()
        for i in overpass_query["elements"]:
            try:
                self.osmStops[str(i["tags"]["ref"])] = i["id"]
            except KeyError:
                continue

    def _routeBetweenStops(self, start_stop, end_stop):
        # Find nodes
        start_lat, start_lon = map(float, self.stops[start_stop])
        end_lat, end_lon = map(float, self.stops[end_stop])

        # Start node
        if self.transport == "bus" and self.osmStops.get(start_stop, None) in self.router.rnodes:
            start = self.osmStops[start_stop]

        else:
            start = self.router.findNode(start_lat, start_lon)

        # End node
        if self.transport == "bus" and self.osmStops.get(end_stop, None) in self.router.rnodes:
            end = self.osmStops[end_stop]

        else:
            end = self.router.findNode(end_lat, end_lon)

        # Do route

        # SafetyCheck - start and end nodes have to be defined
        if start and end:
            try:
                with TimeLimit(10):
                    status, route = self.router.doRoute(start, end)
            except TimeoutError:
                status, route = "timeout", []

            route_points = list(map(self.router.nodeLatLon, route))

            try: dist_ratio = _TotalDistance(route_points) / _Distance([start_lat, start_lon], [end_lat, end_lon])
            except ZeroDivisionError: dist_ratio = _TotalDistance(route_points) / 0.001

            # SafetyCheck - route has to have at least 2 nodes
            if status == "success" and len(route_points) <= 1:
                status = "to_few_nodes_(%d)" % len(route)

            # SafetyCheck - route can't be unbelivabely long than straight line between stops
            # Except for stops in same stop group
            elif start_stop[:4] == end_stop[:4] and dist_ratio > self.ratios.get(start_stop + "-" + end_stop, 7):
                status = "route_too_long_in_group_ratio:%s" % round(dist_ratio, 2)

            elif start_stop[:4] != end_stop[:4] and dist_ratio > self.ratios.get(start_stop + "-" + end_stop, 3.5):
                status = "route_too_long_ratio:%s" % round(dist_ratio, 2)

            # Apply rdp algorithm
            route_points = rdp(route_points, epsilon=0.000006)

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
                             "geometry": {"type":"LineString", "coordinates":[[i[1], i[0]] for i in route_points]}
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
                dist += _Distance((route_points[x-1][0], route_points[x-1][1]), (route_points[x][0], route_points[x][1]))
                route_points[x].append(dist)

        return route_points

    def nextRoute(self, short_name, transport):
        self.trips.clear()

        if transport == "0": transport = "tram"
        elif transport == "3": transport = "bus"
        elif transport == "2": transport = "train"
        else: raise ValueError("Invalid transport type {} for Shaper".format(transport))

        if transport != self.transport:
            temp_xml = NamedTemporaryFile(delete=False)
            if transport == "train":
                request = requests.get("https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm")

            elif transport == "tram":
                request = requests.get("https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm")

            else:
                # That's an overpass query for highways around Warsaw metro area
                request = requests.get(r"https://overpass-api.de/api/interpreter/?data=%5Bbbox%3A51.92%2C20.46%2C52.49%2C21.465%5D%5Bout%3Axml%5D%3B%0A(%0A%20way%5B%22highway%22%3D%22motorway%22%5D%3B%0A%20way%5B%22highway%22%3D%22motorway_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22unclassified%22%5D%3B%0A%20way%5B%22highway%22%3D%22minor%22%5D%3B%0A%20way%5B%22highway%22%3D%22residential%22%5D%3B%0A%20way%5B%22highway%22%3D%22living_street%22%5D%3B%0A%20way%5B%22highway%22%3D%22service%22%5D%3B%0A)%3B%0A(._%3B%3E%3B)%3B%0Aout%3B")

                # And this one also contains turn restrictions
                #request = requests.get(r"https://overpass-api.de/api/interpreter/?data=%5Bbbox%3A51.92%2C20.46%2C52.49%2C21.465%5D%5Bout%3Axml%5D%3B%0A(%0A%20way%5B%22highway%22%3D%22motorway%22%5D%3B%0A%20way%5B%22highway%22%3D%22motorway_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22unclassified%22%5D%3B%0A%20way%5B%22highway%22%3D%22minor%22%5D%3B%0A%20way%5B%22highway%22%3D%22residential%22%5D%3B%0A%20way%5B%22highway%22%3D%22living_street%22%5D%3B%0A%20way%5B%22highway%22%3D%22service%22%5D%3B%0A)%3B%0A%3E-%3E.n%3B%0A%3C-%3E.r%3B%0A(._%3B.n%3B.r%3B)%3B%0Aout%3B%0A")

            temp_xml.write(request.content)
            self.router = pyroutelib3.Router(transport, temp_xml.name)
            temp_xml.close()

        self.transport = transport

    def get(self, trip_id, stops):
        pattern_id = trip_id.split("/")[0] + "/" + trip_id.split("/")[1]

        if pattern_id in self.trips:
            return pattern_id, self.trips[pattern_id]

        elif not self.router:
            return "", {}

        pt_seq = 0
        dist = 0.0
        distances = {0: 0.0}

        routes = []
        for i in range(1, len(stops)):
            routes.append(self._routeBetweenStops(stops[i-1], stops[i]))

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

    def close(self):
        self.file.close()

class Parser:
    def __init__(self, file, shapes=False):
        # File-like object
        if hasattr(file, "read") and hasattr(file, "seek"):
            file.seek(0)
            self.reader = file

        # Path-like object/String
        else:
            self.reader = open(os.fspath(file), mode="r", encoding="cp1250")

        if not os.path.exists("gtfs"):
            os.makedirs("gtfs")

        self.shapes = Shaper() if shapes else None

        self.verbose = False
        self.stop_names = _ProperStopNames()
        self.stops_map = {}
        self.stop_positions = {}
        self.unused_stops = set()
        self.incorrect_stops = []
        self.active_rail_stops = {
            "4900", "4901", "7900", "7901", "7902", "2901", "2900", "2918", "2917", "2916", "2915",
            "2909", "2908", "2907", "2906", "2905", "2904", "2903", "2902", "4902", "4903", "4923",
            "4904", "4905", "2914", "2913", "2912", "2911", "2910", "4919", "3901", "4918", "4917",
            "4913", "1910", "1909", "1908", "1907", "1906", "1905", "1904", "1903", "1902", "1901",
            "7903", "5907", "5904", "5903", "5902"
            # List of rail stops used by S× lines. Other rail stops are ignored.
        }

    def parse_KA(self):
        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf8", newline="")
        writer = csv.writer(file)
        writer.writerow(["service_id", "date", "exception_type"])
        for line in self.reader:
            line = line.strip()
            if not line: continue

            # End Of Section
            if line == "#KA":
                file.close()
                return

            else:
                line_splitted = line.split()
                date = line_splitted[0].replace("-", "")
                for service_id in line_splitted[2:]:
                    writer.writerow([service_id, date, "1"])

        raise EOFError("End of section KA not reached!")

    def parse_ZP(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "railway_pkpplk_id", "platform_code", "wheelchair_boarding"])

        inside_group = False

        # Load info about missing stops
        missing_stops = requests.get("https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/raw/missing_stops.json").json()
        rail_platforms = requests.get("https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/raw/rail_platforms.json").json()

        self.unused_stops = set(missing_stops.keys())

        for line in self.reader:
            line = line.strip()

            zp_match = re.match(r"(\d{4})\s+([^,]{,30})[\s,]+([\w-]{2})\s+(.*)", line)
            pr_match = re.match(r"(\d{4})(\d{2}).+Y=\s?([0-9.]+|[Yy.]+)\s+X=\s?([0-9.]+|[Xx.]+)", line)

            # End of section
            if line.startswith("#ZP"):
                return

            # Data parser
            elif zp_match:
                stops_in_group = OrderedDict()
                virtual_stops_in_group = OrderedDict()
                group_ref = zp_match[1]
                group_name = self.stop_names.get(group_ref, _NormalizeName(zp_match[2]))
                group_town = zp_match[4].title()

                if _AddTownToStopName(group_ref, group_name, group_town, zp_match[3]):
                    group_name = group_town + " " + group_name
                    self.stop_names[group_ref] = group_name

            elif inside_group and pr_match:
                stop_ref = pr_match[2]
                lat, lon = pr_match[3], pr_match[4]

                # Case: virtual stop
                if stop_ref[0] == "8":
                    virtual_stops_in_group[stop_ref] = [lat, lon]

                # Case: No location
                elif re.match(r"[Yy.]+", lat) or re.match(r"[Xx.]+", lon):
                    # Sub-case: data in missing_stops
                    if group_ref+stop_ref in missing_stops.keys():
                        self.unused_stops.remove(group_ref+stop_ref)
                        stops_in_group[stop_ref] = missing_stops[group_ref+stop_ref]

                    # Sub-case: no data available
                    else:
                        self.incorrect_stops.append(group_ref+stop_ref)
                        self.stops_map[group_ref+stop_ref] = None

                # Case: position defined
                else:
                    stops_in_group[stop_ref] = lat, lon

            # Section changes
            elif line.startswith("*PR"):
                inside_group = True

            # Export data from stop_group
            elif line.startswith("#PR"):
                inside_group = False

                # Case: unused rail stops
                if group_ref[1:3] in {"90", "91", "92"} and group_ref not in self.active_rail_stops:
                    for stop_ref in list(stops_in_group.keys()) + list(virtual_stops_in_group.keys()):
                        self.stops_map[group_ref+stop_ref] = None

                # Case: used rail stops with platform data
                elif group_ref[1:3] in {"90", "91", "92"} and group_ref in rail_platforms.keys():
                    station_info = rail_platforms[group_ref]
                    station_lat, station_lon = station_info["pos"].split(",")

                    # Sub-case: one platform station
                    if station_info["oneplatform"]:
                        # Write info
                        if self.shapes: self.shapes.stops[group_ref] = station_lat, station_lon
                        writer.writerow([
                            group_ref, station_info["name"], station_lat, station_lon, "",
                            "", station_info["pkpplk_code"], "1", station_info["wheelchair"]
                        ])

                        # Add info to stop_map table
                        for stop_ref in list(stops_in_group.keys()) + list(virtual_stops_in_group.keys()):
                            self.stops_map[group_ref+stop_ref] = group_ref

                    # Sub-case: many patforms
                    else:
                        # Hub entry
                        writer.writerow([
                            group_ref, station_info["name"], station_lat, station_lon, "1",
                            "", station_info["pkpplk_code"], "", station_info["wheelchair"]
                        ])

                        # Platform entries
                        for platform_ref, platform_pos in station_info["platforms"].items():
                            platform_lat, platform_lon = platform_pos.split(",")
                            platform_code = platform_ref.split("p")[1]
                            if self.shapes: self.shapes.stops[platform_ref] = platform_lat, platform_lon
                            writer.writerow([
                                platform_ref, station_info["name"] + " peron " + platform_code, platform_lat, platform_lon, "0",
                                group_ref, station_info["pkpplk_code"], platform_code, station_info["wheelchair"]
                            ])

                        # Add info to stop_map table
                        for stop_ref in list(stops_in_group.keys()) + list(virtual_stops_in_group.keys()):
                            if group_ref+stop_ref in station_info["stops"]:
                                self.stops_map[group_ref+stop_ref] = station_info["stops"][group_ref+stop_ref]
                            else:
                                if (group_ref+stop_ref) not in ["491303", "491304"]:
                                    warn("No platform defined for railway 'stop' {} {}".format(station_info["name"], stop_ref))
                                self.stops_map[group_ref+stop_ref] = group_ref

                # Case: used rail stop with no platform data
                elif group_ref[1:3] in {"90", "91", "92"}:
                    station_lat, station_lon = _GroupAvgPosition(stops_in_group)
                    if self.shapes: self.shapes.stops[group_ref] = station_lat, station_lon
                    writer.writerow([group_ref, group_name, station_lat, station_lon, "", "", "", "", ""])

                    for stop_ref in list(stops_in_group.keys()) + list(virtual_stops_in_group.keys()):
                        self.stops_map[group_ref+stop_ref] = group_ref

                # Case: normal stop group
                else:
                    # Well-defined stops
                    for stop_ref, stop_pos in stops_in_group.items():
                        if self.shapes: self.shapes.stops[group_ref+stop_ref] = stop_pos[0], stop_pos[1]
                        writer.writerow([group_ref+stop_ref, group_name + " " + stop_ref, stop_pos[0], stop_pos[1], "", "", "", "", ""])

                    # Virtual stops
                    for stop_ref, stop_pos in virtual_stops_in_group.items():
                        refs_by_pos = [k for k, v in stops_in_group.items() if v == stop_pos]
                        refs_by_digit = [k for k in stops_in_group.keys() if k[1] == stop_ref[1]]

                        # There exists a stop with the same location
                        if refs_by_pos:
                            self.stops_map[group_ref+stop_ref] = group_ref+refs_by_pos[0]

                        # Edge case Metro Młociny 88 → Metro Młociny 28
                        elif group_ref+stop_ref == "605988" and "28" in stops_in_group.keys():
                            self.stops_map["605988"] = "605928"

                        # There exists a stop with similat last digit
                        elif refs_by_digit:
                            self.stops_map[group_ref+stop_ref] = group_ref+refs_by_digit[0]

                        # No ability to map virtual stop
                        else:
                            self.stops_map[group_ref+stop_ref] = None
                            self.incorrect_stops.append(group_ref+stop_ref)

        raise EOFError("End of section ZP was not reached before EOF!")

    def parse_TR(self, route_id):
        route_name = ""
        inaccesible_trips = set()
        on_demand_stops = set()
        direction_stops = {"A": set(), "B": set()}

        inside_lw = False
        inside_wg = False
        inside_od = False

        for line in self.reader:
            line = line.strip()

            tr_match = re.match(r"([\w-]+)\s*,\s+([^,]{,30})[\s,]+([\w-]{2})\s+==>\s+([^,]{,30})[\s,]+([\w-]{2})\s+Kier\. (\w)\s+Poz. (\w)", line)
            lw_match = re.match(r".*(\d{6})\s+[^,]{,30}[\s,]+[\w-]{2}\s+\d\d\s+(NŻ|)\s*\|.*", line)
            wg_match = re.match(r"G\s+\d*\s+(\d*):\s+(.+)", line)
            od_match = re.match(r"([\d.]+)\s+(.+){,17}", line)

            if tr_match:
                direction, pattern_order = tr_match[6], tr_match[7]

            elif line.startswith("*LW"):
                inside_lw = True
                if not route_name: pattern_stops = []

            elif inside_lw and lw_match:
                direction_stops[direction].add(lw_match[1])
                if not route_name: pattern_stops.append(lw_match[1])
                if lw_match[2] == "NŻ": on_demand_stops.add(lw_match[1])

            elif inside_lw and line.startswith("#LW"):
                inside_lw = False
                if not route_name:
                    first_stop = self.stop_names.get(pattern_stops[0][:4], "")
                    last_stop = self.stop_names.get(pattern_stops[-1][:4], "")
                    if first_stop and last_stop: route_name = first_stop + " — " + last_stop
                    else: warn("No route_long_name for route {}".format(route_id))

            elif line.startswith("*WG"):
                inside_wg = True
                hour = "0"
                inaccesible_times = set()

            elif inside_wg and wg_match:
                if int(hour) > int(wg_match[1]): hour = str(24+int(wg_match[1]))
                else: hour = wg_match[1]
                for minutes in wg_match[2].split():
                    # "[" before minutres indicates that this departure is accessible
                    if not minutes.startswith("["):
                        inaccesible_times.add(hour + "." + re.sub(r"\D", "", minutes))

            elif inside_wg and line.startswith("#WG"):
                inside_wg = False

            elif line.startswith("*OD"):
                inside_od = True

            elif inside_od and od_match:
                if od_match[1] in inaccesible_times:
                    inaccesible_trips.add(od_match[2])

            elif inside_od and line.startswith("#OD"):
                inside_od = False

            elif line.startswith("#TR"):
                return route_name, inaccesible_trips, on_demand_stops, direction_stops

        raise EOFError("End of section TR (route {}) was not reached before EOF!".format(route_id))

    def parse_WK(self, route_id):
        trips = OrderedDict()
        for line in self.reader:
            line = line.strip()
            wk_match = re.match(r"(\S{,17})\s+(\d{6})\s+\w{2}\s+([0-9.]+)(\s+\w|)", line)

            if wk_match:
                trip_id = wk_match[1]
                stop = self.stops_map.get(wk_match[2], wk_match[2])
                if not stop: continue
                if trip_id not in trips: trips[trip_id] = []
                trips[trip_id].append({"stop": stop, "original_stop": wk_match[2], "time": _NormalizeTime(wk_match[3]), "flags": wk_match[4]})

            elif line.startswith("#WK"):
                return trips

        raise EOFError("End of section WK (route {}) was not reached before EOF!".format(route_id))

    def parse_LL(self):
        file_routes = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer_routes = csv.writer(file_routes)
        writer_routes.writerow(["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color", "route_sort_order"])

        file_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.writer(file_trips)
        writer_trips.writerow(["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id", "exceptional", "wheelchair_accessible", "bikes_allowed"])

        file_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.writer(file_times)
        writer_times.writerow(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled"])

        route_sort_order = 2 # Leave first 2 blank for M1 and M2 routes
        route_id = None

        for line in self.reader:
            line = line.strip()
            ll_match = re.match(r"Linia:\s+([A-Za-z0-9-]{1,3})  - (.+)", line)

            # Load basic info on route
            if ll_match:
                route_id = ll_match[1]

                # Ignore Koleje Mazowieckie & Warszawska Kolej Dojazdowa routes
                if route_id.startswith("R") or route_id.startswith("WKD"):
                    route_id, route_type, route_color, route_text = None, None, None, None

                else:
                    if self.verbose: print("\033[1A\033[KParsing section 'LL' - route {}".format(route_id))
                    route_sort_order += 1
                    route_type, route_color, route_text_color = _RouteColorType(route_id, ll_match[2])
                    if self.shapes: self.shapes.nextRoute(route_id, route_type)

            # Parse section TR - some useful data for later use
            elif line.startswith("*TR") and route_id:
                route_name, inaccesible_trips, on_demand_stops, direction_stops = self.parse_TR(route_id)

            # Parse section WK - stop_times && Output data to GTFS
            elif line.startswith("*WK") and route_id:
                trips = self.parse_WK(route_id)

                writer_routes.writerow(["0", route_id, route_id, route_name, route_type, route_color, route_text_color, route_sort_order])

                for trip_id, trip_stops in trips.items():
                    if len(trip_stops) > 1:
                        # Basic data
                        service_id = trip_id.split("/")[1]
                        full_trip_id = route_id + "/" + trip_id
                        excpetional = "0" if (trip_id.startswith("TP-") or trip_id.startswith("TO-")) else "1"

                        # Shape
                        if self.shapes: shape_id, shape_distances = self.shapes.get(full_trip_id, [i["stop"] for i in trip_stops])
                        else: shape_id, shape_distances = "", {}

                        # Wheelchair Accessibility
                        if trip_id in inaccesible_trips: wheelchair_accessible = "2"
                        else: wheelchair_accessible = "1"

                        # Direction
                        single_direction_stops = {i["original_stop"] for i in trip_stops} & (direction_stops["A"] ^ direction_stops["B"])
                        direction_a = len(single_direction_stops & direction_stops["A"])
                        direction_b = len(single_direction_stops & direction_stops["B"])

                        if not single_direction_stops:
                            direction_id = "0"
                        elif direction_a >= direction_b:
                            direction_id = "0"
                        elif direction_a < direction_b:
                            direction_id = "1"
                        else:
                            direction_id = ""
                            warn("No direction_id for trip {}/{}".format(route_id, trip_id))

                        # Headsign
                        headsign = _Headsign(trip_stops[-1]["stop"], self.stop_names.get(trip_stops[-1]["stop"][:4], ""))
                        if not headsign:
                            warn("No headsign for trip {}".format(full_trip_id))

                        # Output to trips.txt
                        writer_trips.writerow([
                            route_id, service_id, full_trip_id, headsign, direction_id,
                            shape_id, excpetional, wheelchair_accessible, "1"
                        ])

                        # Output to stop_times.txt
                        for sequence, stop_time in enumerate(trip_stops):
                            # Pickup type
                            if sequence == len(trip_stops)-1: pickup = "1"
                            elif stop_time["original_stop"] in on_demand_stops: pickup = "3"
                            else: pickup = "0"

                            # Drop-off type
                            if sequence == 0: dropoff = "1"
                            elif "P" in stop_time["flags"]: dropoff = "1"
                            elif stop_time["original_stop"] in on_demand_stops: dropoff = "3"
                            else: dropoff = "0"

                            # shape_dist_traveled
                            stop_dist = shape_distances.get(sequence, "")
                            if stop_dist: stop_dist = round(stop_dist, 4)

                            # Output to stop_times.txt
                            writer_times.writerow([
                                full_trip_id, stop_time["time"], stop_time["time"], stop_time["stop"],
                                sequence + 1, pickup, dropoff, stop_dist
                            ])

            # End of LL section
            elif line.startswith("#LL"):
                file_routes.close()
                file_trips.close()
                file_times.close()
                if self.shapes: self.shapes.close()
                return

        raise EOFError("End of section LL was not reached before EOF!")

    def parse(self, verbose=False):
        self.verbose = verbose

        for line in self.reader:
            line = line.strip()

            if line.startswith("*KA"):
                if self.verbose: print("Parsing section 'KA' - calendars")
                self.parse_KA()

            elif line.startswith("*ZP"):
                if self.verbose: print("Parsing section 'ZP' - stops")
                self.parse_ZP()

            elif line.startswith("*LL"):
                if self.verbose: print("Parsing section 'LL' - schedules")
                self.parse_LL()
                if self.verbose: print("\033[1A\033[KParsing section 'LL' - schedules")

    def missing_stops(self):
        with open("missing_stops.json", "w") as f:
            json.dump({"missing": list(map(int, self.incorrect_stops)), "unused": sorted(map(int, self.unused_stops))}, f, indent=0)

def main(shapes=False, metro=False, verbose=False, date=None, prevver=""):
    print("Downloading file")
    txt_file, version = Download(date)

    if prevver == version:
        print("File matches the 'prevver' argument, aborting!")
        txt_file.close()
        return

    print("Cleaning directories")
    ClearDir(shapes)

    if shapes: print("Initilaizing parser ({}) this can take up to 30 mins".format(version))
    else: print("Initilaizing parser ({}) this can take up to 5 mins".format(version))
    parser = Parser(txt_file, shapes)
    parser.parse(verbose=verbose)

    print("Parser finished working, closing TXT file")
    txt_file.close()
    parser.missing_stops()

    print("Creating static files")
    StaticFiles(version, shapes)

    if metro:
        print("Adding metro")
        Metro.add()

    print("Compressing")
    Compress()

    return version

if __name__ == "__main__":
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-s", "--shapes", action="store_true", required=False, dest="shapes", help="generate shapes based on OSM data. available only on Unix systems")
    argprs.add_argument("-v", "--verbose", action="store_true", required=False, dest="verbose", help="print out some more information about what the parser is doing")
    argprs.add_argument("-m", "--metro", action="store_true", required=False, dest="metro", help="append metro schedules from mkuran.pl")
    argprs.add_argument("-d", "--date", default=None, required=False, metavar="yymmdd", dest="date", help="date for which schedules should be downloaded, if not today")
    argprs.add_argument("-p", "--prevver", default="", required=False, metavar="RAyymmdd", dest="prevver", help="previous feed_version, if you want to avoid downloading the same file again")
    args = argprs.parse_args()
    print("""
    . . .                         ,---.--.--,---.,---.
    | | |,---.,---.,---.,---.. . .|  _.  |  |__. `---.
    | | |,---||    `---.,---|| | ||   |  |  |        |
    `-'-'`---^`    `---'`---^`-'-'`---'  `  `    `---'
    """)

    main(args.shapes, args.metro, args.metro, args.date, args.prevver)

    print("=== Done! ===")
    print("Time elapsed: %s s" % round(time.time() - st, 3))
