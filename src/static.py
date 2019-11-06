from collections import OrderedDict
from tempfile import NamedTemporaryFile
from warnings import warn
from datetime import date
from ftplib import FTP
import libarchive.public
import requests
import json
import csv
import re
import os

from .utils_static import *
from .parser import Parser
from .utils import *

# List of rail stops used by S× lines. Other rail stops are ignored.
ACTIVE_RAIL_STATIONS = {
    "4900", "4901", "7900", "7901", "7902", "2901", "2900", "2918", "2917", "2916", "2915",
    "2909", "2908", "2907", "2906", "2905", "2904", "2903", "2902", "4902", "4903", "4923",
    "4904", "4905", "2914", "2913", "2912", "2911", "2910", "4919", "3901", "4918", "4917",
    "4913", "1910", "1909", "1908", "1907", "1906", "1905", "1904", "1903", "1902", "1901",
    "7903", "5908", "5907", "5904", "5903", "5902"
}

PROPER_STOP_NAMES = {
    "4040": "Lotnisko Chopina",              "1484": "Dom Samotnej Matki",
    "2005": "Praga-Płd. - Ratusz",           "1541": "Marki Bandurskiego I",
    "5001": "Połczyńska - Parking P+R",      "2296": "Szosa Lubelska",
    "6201": "Lipków Paschalisa-Jakubowicza", "1226": "Mańki-Wojody",
}

class Converter:
    def __init__(self, version="", shapes=False, clear_shape_errors=True):
        clear_directory("gtfs")
        if clear_shape_errors: clear_directory("shape-errors")

        # Stop info
        self.missing_stops = requests.get("https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/raw/missing_stops.json").json()
        self.rail_platforms = requests.get("https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/raw/rail_platforms.json").json()

        self.incorrect_stops = []
        self.unused_stops = list(self.missing_stops.keys())
        self.stops_map = {}

        self.stop_names = PROPER_STOP_NAMES.copy()

        # File handler
        self.version = None
        self.reader = None
        self.parser = None

        # Get shape generator instance
        if isinstance(shapes, Shaper):
            self.shapes = shapes
            self.shapes.open()

        elif shapes:
            self.shapes = Shaper()
            self.shapes.open()

        else:
            self.shapes = None

        self.get_file(version)

    def get_file(self, version):
        "Download and decompress schedules for current data. Returns tuple (TemporaryFile, version) - and that TemporaryFile is decompressed .TXT file"
        # Login to ZTM server and get the list of files
        server = FTP("rozklady.ztm.waw.pl")
        server.login()
        files = [f for f in server.nlst() if re.fullmatch(r"RA\d{6}\.7z", f)]

        # If user has requested an exact version, check if it's on the server
        if version:
            fname = "{}.7z".format(version)
            if fname not in files:
                raise KeyError("Requested file version ({}) not found on ZTM server".format(version))

        # If not, find one valid today
        else:
            fdate = date.today()
            while True:
                fname = fdate.strftime("RA%y%m%d.7z")
                if fname in files: break
                else: fdate -= timedelta(days=1)

        # Create temporary files for storing th 7z archive and the compressed TXT file
        temp_arch = NamedTemporaryFile(mode="w+b", delete=False)
        self.reader = NamedTemporaryFile(mode="w+t", delete=True)

        try:
            # Download the file
            server.retrbinary("RETR " + fname, temp_arch.write)
            server.quit()
            temp_arch.close()

            # Open the temporary archive inside
            with libarchive.public.file_reader(temp_arch.name) as arch:

                # Iterate over each file inside the archive
                for arch_file in arch:

                    # Assert the file inside the archive is the TXT file we're looking for
                    name_match = re.fullmatch(r"(RA\d{6})\.TXT", arch_file.pathname, flags=re.IGNORECASE)
                    if not name_match:
                        continue

                    # Save the feed version
                    self.version = name_match[1].upper()

                    # Decompress the TXT file block by block and save it to the reader
                    for block in arch_file.get_blocks():
                        self.reader.write(str(block, "cp1250"))
                    self.reader.seek(0)

                    # only one TXT file should be inside the archive
                    break

                else:
                    raise FileNotFoundError("no schedule file found inside archive {}".format(fname))

        # Remove the temp arch file at the end
        finally:
            os.remove(temp_arch.name)

        self.parser = Parser(self.reader)

    def calendar(self):
        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf8", newline="")
        writer = csv.writer(file)
        writer.writerow(["service_id", "date", "exception_type"])

        print("\033[1A\033[K" + "Parsing calendars (KA)")

        for day in self.parser.parse_ka():
            for service_id in day["services"]:
                writer.writerow([service_id, day["date"], "1"])

        file.close()

    def _stopgroup_railway(self, writer, group_id, group_name):
        # Load ZTM stakes from PR section
        # self.parser.parse_pr() has to be called to skip to the next entry in ZP
        stakes = list(self.parser.parse_pr())

        # If group is not in ACTIVE_RAIL_STATIONS, ignore it
        if group_id not in ACTIVE_RAIL_STATIONS:
            for s in stakes: self.stops_map[s["id"]] = None
            return

        # Basic info about the station
        station_info = self.rail_platforms.get(group_id, {})

        # If this station is not in rail_platforms, average all stake positions
        # In order to calculate an approx position of the station
        if not station_info:
            stake_positions = [(i["lat"], i["lon"]) for i in stakes]
            stake_positions = [i for i in stake_positions if i[0] and i[1]]

            if stake_positions:
                station_lat, station_lon = avg_position(stake_positions)

            # No position for the station
            else:
                for s in stakes: self.stops_map[s["id"]] = None
                self.incorrect_stops.append(group_id)
                return

        # Otherwise get the position from rail_platforms data
        else:
            station_lat, station_lon = map(float, station_info["pos"].split(","))
            group_name = station_info["name"]

        # One Platform or No Platform data
        if (not station_info) or station_info["oneplatform"]:
            # Save position for shapes
            if self.shapes:
                self.shapes.stops[group_id] = station_lat, station_lon

            # Add info for stops_map
            for stake in stakes:
                self.stops_map[stake["id"]] = group_id

            # Output info to GTFS
            writer.writerow([
                group_id, group_name, station_lat, station_lon,
                "", "", station_info.get("ibnr_code", ""),
                "", station_info.get("wheelchair", 0),
            ])

        # Multi-Platform station
        else:
            # Hub entry
            writer.writerow([
                group_id, group_name, station_lat, station_lon,
                "1", "", station_info["ibnr_code"],
                "", station_info.get("wheelchair", 0),
            ])

            # Platforms
            for platform_id, platform_pos in station_info["platforms"].items():
                platform_lat, platform_lon = map(float, platform_pos.split(","))
                platform_code = platform_id.split("p")[1]
                platform_name = f"{group_name} peron {platform_code}"

                # Save position for shapes
                if self.shapes:
                    self.shapes.stops[platform_id] = platform_lat, platform_lon

                # Output to GTFS
                writer.writerow([
                    platform_id, platform_name, platform_lat, platform_lon,
                    "0", group_id, station_info["ibnr_code"],
                    platform_code, station_info.get("wheelchair", 0),
                ])

            # Stops → Platforms
            for stake in stakes:
                # Defined stake in rail_platforms
                if stake["id"] in station_info["stops"]:
                    self.stops_map[stake["id"]] = station_info["stops"][stake["id"]]

                # Unknown stake
                elif stake["id"] not in {"491303", "491304"}:
                    warn(f'No platform defined for railway PR entry {group_name} {stake["id"]}')

    def _stopgroup_normal(self, writer, group_id, group_name):
        # Load ZTM stakes from PR section
        # self.parser.parse_pr() has to be called to skip to the next entry in ZP
        stakes = list(self.parser.parse_pr())

        # Split virtual stakes from normal stakes
        virtual_stakes = [i for i in stakes if i["code"][0] == "8"]
        normal_stakes = [i for i in stakes if i["code"][0] != "8"]


        # Load positions from missing_stops to normal_stakes
        for idx, stake in enumerate(normal_stakes):
            if (stake["lat"] == None or stake["lon"] == None) and \
                                          stake["id"] in self.missing_stops:

                self.unused_stops.remove(stake["id"])
                stake["lat"], stake["lon"] = self.missing_stops[stake["id"]]
                normal_stakes[idx] = stake

        position_stakes = [i for i in normal_stakes if i["lat"] and i["lon"]]

        # Convert normal stakes
        for stake in normal_stakes:

            # Position defined
            if stake["lat"] and stake["lon"]:

                # Save position for shapes
                if self.shapes:
                    self.shapes.stops[stake["id"]] = stake["lat"], stake["lon"]

                # Output info to GTFS
                writer.writerow([
                    stake["id"], f'{group_name} {stake["code"]}',
                    stake["lat"], stake["lon"],
                    "", "", "", "", stake["wheelchair"],
                ])

            # Position undefined
            else:
                self.stops_map[stake["id"]] = None
                self.incorrect_stops.append(stake["id"])

        # Convert virtual stops
        for stake in virtual_stakes:

            stakes_with_same_pos = [i["id"] for i in position_stakes if \
                           (i["lat"], i["lon"]) == (stake["lat"], stake["lon"])]

            stakes_with_same_code = [i["id"] for i in position_stakes if \
                                               i["code"][1] == stake["code"][1]]

            # Metro Młociny 88 → Metro Młociny 28
            if stake["id"] == "605988":
                counterpart_available = [i for i in position_stakes if \
                                                            i["id"] == "605928"]

                # If 605928 is present, map 605988 to it.
                # Otherwise fallback on defualt maching options
                if counterpart_available:
                    self.stops_map["605988"] = "605928"
                    continue

            # Map to a stake with same position
            if stakes_with_same_pos:
                self.stops_map[stake["id"]] = stakes_with_same_pos[0]

            # Map to a stake with same digit
            elif stakes_with_same_code:
                self.stops_map[stake["id"]] = stakes_with_same_code[0]

            # Unable find a matching stake
            else:
                self.stops_map[stake["id"]] = None
                self.incorrect_stops.append(stake["id"])

    def stops(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "stop_IBNR", "platform_code", "wheelchair_boarding"])

        print("\033[1A\033[K" + "Parsing stops (ZP)")

        for group in self.parser.parse_zp():
            # Fix town name for Kampinoski PN
            if group["town"] == "Kampinoski Pn":
                group["town"]  = "Kampinoski PN"

            # Add name to self.stop_names if it's missing
            if group["id"] not in self.stop_names:
                group["name"] = normal_stop_name(group["name"])
                self.stop_names[group["id"]] = group["name"]

            else:
                group["name"]  = self.stop_names[group["id"]]

            # Add town name to stop name
            if should_town_be_added_to_name(group):
                group["name"] = f'{group["town"]} {group["name"]}'
                self.stop_names[group["id"]] = group["name"]

            # Parse stakes
            if group["id"][1:3] in {"90", "91", "92"}:
                self._stopgroup_railway(writer, group["id"], group["name"])

            else:
                self._stopgroup_normal(writer, group["id"], group["name"])

        file.close()

    def routes_schedules(self):
        file_routes = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer_routes = csv.writer(file_routes)
        writer_routes.writerow(["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color", "route_sort_order"])

        file_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.writer(file_trips)
        writer_trips.writerow(["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id", "exceptional", "wheelchair_accessible", "bikes_allowed"])

        file_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.writer(file_times)
        writer_times.writerow(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled"])

        route_sort_order = 1 # Leave first 2 blank for M1 and M2 routes
        route_id = None

        print("\033[1A\033[K" + "Parsing routes & schedules (LL)")

        for route in self.parser.parse_ll():
            route_id, route_desc = route["id"], route["desc"]

            # Ignore Koleje Mazowieckie & Warszawska Kolej Dojazdowa routes
            if route_id.startswith("R") or route_id.startswith("WKD"):
                self.parser.skip_to_section("WK", end=True)
                continue

            print("\033[1A\033[K" + f"Parsing routes & schedules (LL) - {route_id}")


            route_sort_order += 1
            route_type, route_color, route_text_color = route_color_type(route_id, route_desc)

            # Data loaded from TR section
            route_name = ""
            direction_stops = {"0": set(), "1": set()}
            on_demand_stops = set()
            inaccesible_trips = set()
            variant_directions = {}

            # Variants
            print("\033[1A\033[K" + f"Parsing routes & schedules (TR) - {route_id}")

            for variant in self.parser.parse_tr():
                print("\033[1A\033[K" + f"Parsing routes & schedules (LW) - {route_id}")

                stops = list(self.parser.parse_lw())

                # variant direction
                variant_directions[variant["id"]] = variant["direction"]

                # route_name should be the name of first and last stop of 1st variant
                if not route_name:
                    route_name = " — ".join([
                        self.stop_names[stops[0]["id"][:4]],
                        self.stop_names[stops[-1]["id"][:4]]
                    ])

                # add on_demand_stops from this variant
                on_demand_stops |= {i["id"] for i in stops if i["on_demand"]}

                # add stopids to proper direction in direction_stops
                direction_stops[variant["direction"]] |= {i["id"] for i in stops}

                # now parse ODWG sections - for inaccesible trips (only tram)
                if route_type == "0":
                    print("\033[1A\033[K" + f"Parsing routes & schedules (TD) - {route_id}")

                    for trip in self.parser.parse_wgod(route_type, route_id):
                        if not trip["accessible"]:
                            inaccesible_trips.add(trip["id"])

                else:
                    self.parser.skip_to_section("RP", end=True)

            # Schedules
            print("\033[1A\033[K" + f"Parsing routes & schedules (WK) - {route_id}")

            for trip in self.parser.parse_wk(route_id):

                # Change stop_ids based on stops_map
                for stopt in trip["stops"]:
                    stopt["orig_stop"] = stopt.pop("stop")
                    stopt["stop"] = self.stops_map.get(
                        stopt["orig_stop"], stopt["orig_stop"]
                    )

                # Fliter "None" stops
                trip["stops"] = [i for i in trip["stops"] if i["stop"]]

                # Ignore trips with only 1 stopt
                if len(trip["stops"]) < 2: continue

                # Unpack info from trip_id
                trip_id = trip["id"]

                trip_id_split = trip_id.split("/")
                variant_id = trip_id_split[1]
                service_id = trip_id_split[2]

                del trip_id_split

                # "Exceptional" trip - a deutor/depot run
                if variant_id.startswith("TP-") or variant_id.startswith("TO-"):
                    exceptional = "0"
                else:
                    exceptional = "1"

                # Shapes
                if self.shapes:
                    shape_id, shape_distances = self.shapes.get(
                        route_type, trip_id, [i["stop"] for i in trip["stops"]])

                else:
                    shape_id, shape_distances = "", {}

                # Wheelchair Accessibility
                if trip_id in inaccesible_trips:
                    wheelchair = "2"
                else:
                    wheelchair = "1"

                # Direction
                if variant_id in variant_directions:
                    direction = variant_directions[variant_id]
                else:
                    direction = trip_direction(
                        {i["orig_stop"] for i in trip["stops"]},
                        direction_stops)

                    variant_directions[variant_id] = direction

                # Headsign
                headsign = proper_headsign(
                    trip["stops"][-1]["stop"],
                    self.stop_names.get(trip["stops"][-1]["stop"][:4], ""))

                if not headsign:
                    warn(f"No headsign for trip {trip_id}")

                # Write to trips.txt
                writer_trips.writerow([
                    route_id, service_id, trip_id, headsign, direction,
                    shape_id, exceptional, wheelchair, "1",
                ])

                max_seq = len(trip["stops"]) - 1

                # StopTimes
                for seq, stopt in enumerate(trip["stops"]):
                    # Pickup Type
                    if seq == max_seq: pickup = "1"
                    elif "P" in stopt["flags"]: pickup = "1"
                    elif stopt["orig_stop"] in on_demand_stops: pickup = "3"
                    else: pickup = "0"

                    # Drop-Off Type
                    if seq == 0: dropoff = "1"
                    elif stopt["orig_stop"] in on_demand_stops: dropoff = "3"
                    else: dropoff = "0"

                    # Shape Distance
                    stop_dist = shape_distances.get(seq, "")
                    if stop_dist: stop_dist = round(stop_dist, 4)

                    # Output to stop_times.txt
                    writer_times.writerow([
                        trip_id, stopt["time"], stopt["time"], stopt["stop"],
                        seq, pickup, dropoff, stop_dist
                    ])

            # Output to routes.txt
            writer_routes.writerow([
                "0", route_id, route_id, route_name, route_type,
                route_color, route_text_color, route_sort_order
            ])

        file_routes.close()
        file_trips.close()
        file_times.close()

    def parse(self):
        self.calendar()
        self.stops()
        self.routes_schedules()

    def dump_missing_stops(self):
        with open("missing_stops.json", "w") as f:
            json.dump(
                {
                    "missing": [int(i) for i in self.incorrect_stops],
                    "unused": [int(i) for i in self.unused_stops],
                },
                f,
                indent=0
            )

    @staticmethod
    def static_files(shapes, version, download_time):
        feed_version = "Version {}; downloaded at: {}".format(version, download_time)

        "Create files that don't depend of ZTM file content"
        file = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write('agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n')
        file.write('0,"Warszawski Transport Publiczny","https://wtp.waw.pl",Europe/Warsaw,pl,19 115,"https://www.nowa.wtp.waw.pl/ceny-i-rodzaje-biletow/"\n')
        file.close()

        file = open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write('feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n')
        if shapes: file.write('"GTFS Convert: MKuranowski; Data: ZTM Warszawa; Bus Shapes (under ODbL License): © OpenStreetMap contributors","https://github.com/MKuranowski/WarsawGTFS",pl,{}\n'.format(feed_version))
        else: file.write('"GTFS Convert: MKuranowski; Data: ZTM Warszawa","https://github.com/MKuranowski/WarsawGTFS",pl,{}\n'.format(feed_version))
        file.close()

    @staticmethod
    def compress(target="gtfs.zip"):
        "Compress all created files to gtfs.zip"
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file in os.listdir("gtfs"):
                if file.endswith(".txt"):
                    archive.write(os.path.join("gtfs", file), arcname=file)

    @classmethod
    def create(cls, version="", shapes=False, metro=False, prevver="", targetfile="gtfs.zip", clear_shape_errors=True):
        print("\033[1A\033[K" + "Downloading file")
        download_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self = cls(version, shapes)

        if prevver == self.version:
            self.reader.close()
            print("\033[1A\033[K" + "File matches the 'prevver' argument, aborting!")
            return

        print("\033[1A\033[K" + "Starting parser...")
        self.parse()

        print("\033[1A\033[K" + "Parser finished working, closing TXT file")
        self.reader.close()
        self.dump_missing_stops()

        print("\033[1A\033[K" + "Creating static files")
        self.static_files(bool(self.shapes), self.version, download_time)

        if metro:
            print("\033[1A\033[K" + "Adding metro")
            Metro.add()

        print("\033[1A\033[K" + "Compressing")
        self.compress(targetfile)

        return self.version
