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
from .utils import *

# List of rail stops used by S× lines. Other rail stops are ignored.
ACTIVE_RAIL_STATIONS = {
    "4900", "4901", "7900", "7901", "7902", "2901", "2900", "2918", "2917", "2916", "2915",
    "2909", "2908", "2907", "2906", "2905", "2904", "2903", "2902", "4902", "4903", "4923",
    "4904", "4905", "2914", "2913", "2912", "2911", "2910", "4919", "3901", "4918", "4917",
    "4913", "1910", "1909", "1908", "1907", "1906", "1905", "1904", "1903", "1902", "1901",
    "7903", "5907", "5904", "5903", "5902"
}

PROPER_STOP_NAMES = {
    "4040": "Lotnisko Chopina",         "1484": "Dom Samotnej Matki",
    "2005": "Praga-Płd. - Ratusz",      "1541": "Marki Bandurskiego I",
    "5001": "Połczyńska - Parking P+R", "2296": "Szosa Lubelska",
    "6201": "Lipków Paschalisa-Jakubowicza"
}

class Parser:
    def __init__(self, version="", shapes=False, clear_shape_errors=True):
        self.get_file(version)

        clear_directory("gtfs")
        if clear_shape_errors: clear_directory("shape-errors")

        self.stops_map = {}
        self.unused_stops = set()
        self.incorrect_stops = []

        self.stop_names = PROPER_STOP_NAMES.copy()

        # Get shape generator instance
        if isinstance(shapes, Shaper):
            self.shapes = shapes
            self.shapes.open()

        elif shapes:
            self.shapes = Shaper()
            self.shapes.open()

        else:
            self.shapes = None

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

    def parse_KA(self):
        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf8", newline="")
        writer = csv.writer(file)
        writer.writerow(["service_id", "date", "exception_type"])

        for line in self.reader:
            line = line.strip()
            if not line: continue

            ka_match = re.match(r"(\d{4})-(\d{2})-(\d{2})\s+\d\s+([\w\s]+)", line)

            # End Of Section
            if line.startswith("#KA"):
                file.close()
                return

            elif ka_match:
                date = ka_match[1] + ka_match[2] + ka_match[3]
                for service_id in ka_match[4].split():
                    writer.writerow([service_id, date, "1"])

        raise EOFError("End of section KA not reached!")

    def parse_ZP(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.writer(file)
        writer.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "stop_IBNR", "platform_code", "wheelchair_boarding"])

        inside_group = False

        # Load info about missing stops
        missing_stops = requests.get("https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/raw/missing_stops.json").json()
        rail_platforms = requests.get("https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/raw/rail_platforms.json").json()
        unaccessible_stops = stops_unaccessible()

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
                group_town = zp_match[4].title()

                # Fix group_town for Kampinoski PN
                if group_town == "Kampinoski Pn":
                    group_town = "Kampinoski PN"

                # Add name to self.stop_names if it's missing
                if group_ref not in self.stop_names:
                    group_name = normal_stop_name(zp_match[2])
                    self.stop_names[group_ref] = group_name

                else:
                    group_name = self.stop_names[group_ref]

                if should_town_be_added_to_name(group_ref, group_name, group_town, zp_match[3]):
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
                if group_ref[1:3] in {"90", "91", "92"} and group_ref not in ACTIVE_RAIL_STATIONS:
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
                            "", station_info["ibnr_code"], "1", station_info["wheelchair"]
                        ])

                        # Add info to stop_map table
                        for stop_ref in list(stops_in_group.keys()) + list(virtual_stops_in_group.keys()):
                            self.stops_map[group_ref+stop_ref] = group_ref

                    # Sub-case: many patforms
                    else:
                        # Hub entry
                        writer.writerow([
                            group_ref, station_info["name"], station_lat, station_lon, "1",
                            "", station_info["ibnr_code"], "", station_info["wheelchair"]
                        ])

                        # Platform entries
                        for platform_ref, platform_pos in station_info["platforms"].items():
                            platform_lat, platform_lon = platform_pos.split(",")
                            platform_code = platform_ref.split("p")[1]
                            if self.shapes: self.shapes.stops[platform_ref] = platform_lat, platform_lon
                            writer.writerow([
                                platform_ref, station_info["name"] + " peron " + platform_code, platform_lat, platform_lon, "0",
                                group_ref, station_info["ibnr_code"], platform_code, station_info["wheelchair"]
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
                    station_lat, station_lon = avg_position(stops_in_group)
                    if self.shapes: self.shapes.stops[group_ref] = station_lat, station_lon
                    writer.writerow([group_ref, group_name, station_lat, station_lon, "", "", "", "", ""])

                    for stop_ref in list(stops_in_group.keys()) + list(virtual_stops_in_group.keys()):
                        self.stops_map[group_ref+stop_ref] = group_ref

                # Case: normal stop group
                else:
                    # Well-defined stops
                    for stop_ref, stop_pos in stops_in_group.items():
                        if self.shapes: self.shapes.stops[group_ref+stop_ref] = stop_pos[0], stop_pos[1]

                        # Accessibility of this stop
                        wheelchair_boarding = "2" if (group_ref+stop_ref) in unaccessible_stops else "1"

                        # Write to stops.txt
                        writer.writerow([group_ref+stop_ref, group_name + " " + stop_ref, stop_pos[0], stop_pos[1], "", "", "", "", wheelchair_boarding])

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
                trips[trip_id].append({"stop": stop, "original_stop": wk_match[2], "time": normal_time(wk_match[3]), "flags": wk_match[4]})

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
                    print("\033[1A\033[K" + "Parsing section 'LL' - route {}".format(route_id))
                    route_sort_order += 1
                    route_type, route_color, route_text_color = route_color_type(route_id, ll_match[2])

            # Parse section TR - some useful data for later use
            elif line.startswith("*TR") and route_id:
                print("\033[1A\033[K" + "Parsing section 'TR' - route {}".format(route_id))
                route_name, inaccesible_trips, on_demand_stops, direction_stops = self.parse_TR(route_id)

            # Parse section WK - stop_times && Output data to GTFS
            elif line.startswith("*WK") and route_id:
                print("\033[1A\033[K" + "Parsing section 'WK' - route {}".format(route_id))
                trips = self.parse_WK(route_id)

                writer_routes.writerow(["0", route_id, route_id, route_name, route_type, route_color, route_text_color, route_sort_order])

                for trip_id, trip_stops in trips.items():
                    if len(trip_stops) > 1:
                        # Basic data
                        service_id = trip_id.split("/")[1]
                        full_trip_id = route_id + "/" + trip_id
                        excpetional = "0" if (trip_id.startswith("TP-") or trip_id.startswith("TO-")) else "1"

                        # Shape
                        if self.shapes: shape_id, shape_distances = self.shapes.get(route_type, full_trip_id, [i["stop"] for i in trip_stops])
                        else: shape_id, shape_distances = "", {}

                        # Wheelchair Accessibility
                        # Only some tram trips are inaccesible
                        # All busses and trains are accessible
                        if trip_id in inaccesible_trips and route_type == "0":
                            wheelchair_accessible = "2"
                        else:
                            wheelchair_accessible = "1"

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
                        headsign = proper_headsign(trip_stops[-1]["stop"], self.stop_names.get(trip_stops[-1]["stop"][:4], ""))
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
                            elif "P" in stop_time["flags"]: pickup = "1"
                            elif stop_time["original_stop"] in on_demand_stops: pickup = "3"
                            else: pickup = "0"

                            # Drop-off type
                            if sequence == 0: dropoff = "1"
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

    def parse(self):
        for line in self.reader:
            line = line.strip()

            if line.startswith("*KA"):
                print("\033[1A\033[K" + "Parsing section 'KA' - calendars")
                self.parse_KA()

            elif line.startswith("*ZP"):
                print("\033[1A\033[K" + "Parsing section 'ZP' - stops")
                self.parse_ZP()

            elif line.startswith("*LL"):
                print("\033[1A\033[K" + "Parsing section 'LL' - schedules")
                self.parse_LL()
                print("\033[1A\033[K" + "Parsing section 'LL' - schedules")

    def missing_stops(self):
        with open("missing_stops.json", "w") as f:
            json.dump({"missing": list(map(int, self.incorrect_stops)), "unused": sorted(map(int, self.unused_stops))}, f, indent=0)

    @staticmethod
    def static_files(shapes, version, download_time):
        feed_version = "Version {}; downloaded at: {}".format(version, download_time)

        "Create files that don't depend of ZTM file content"
        file = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write('agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n')
        file.write('0,"Warszawski Transport Publiczny","http://nowa.wtp.waw.pl",Europe/Warsaw,pl,19 115,"https://www.nowa.wtp.waw.pl/ceny-i-rodzaje-biletow/"\n')
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
        self.missing_stops()

        print("\033[1A\033[K" + "Creating static files")
        self.static_files(bool(self.shapes), self.version, download_time)

        if metro:
            print("\033[1A\033[K" + "Adding metro")
            Metro.add()

        print("\033[1A\033[K" + "Compressing")
        self.compress(targetfile)

        return self.version
