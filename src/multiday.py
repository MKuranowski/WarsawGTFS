from collections import OrderedDict
from itertools import chain
from datetime import date, datetime, timedelta
from ftplib import FTP
import zipfile
import csv
import io
import os
import re

from .utils_static import *
from .static import *
from .utils import *

FILES_TO_COPY = {
    "calendar_dates.txt": ["date", "service_id", "exception_type"],
    "shapes.txt": ["shape_id", "shape_pt_sequence", "shape_dist_traveled", "shape_pt_lat", "shape_pt_lon"],
    "trips.txt": ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id", "exceptional", "wheelchair_accessible", "bikes_allowed"],
    "stop_times.txt": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled"]
}

class MultiDay:
    def __init__(self):
        self.files = None
        self.updated = False

        self.routes = None
        self.stops = None
        self.stop_conversion = None

    def list_feeds(self, maxfiles=10):

        print("\033[1A\033[K" + "Listing feeds to create")

        today = date.today()

        with FTP("rozklady.ztm.waw.pl") as server:
            server.login()

            # Ignore any non-schedule files, sort server files by date (= alphabetical order thanks to %y%m%d)
            server_files = sorted(
                [f for f in server.mlsd() if re.fullmatch(r"RA\d{6}\.7z", f[0])],
                key=lambda i: i[0]
            )

        self.files = []

        for index, (file_name, file_attr) in enumerate(server_files):
            file_start = datetime.strptime(file_name, "RA%y%m%d.7z").date()

            # We don't need anything for previous dates
            if file_start < today: continue

            # Get last day when file is active (next file - 1 day)
            try:
                file_end = datetime.strptime(server_files[index + 1][0], "RA%y%m%d.7z").date() - timedelta(days=1)

            except IndexError:
                file_end = date.max

            # Get file modification time (to recreate GTFS if a newer version was uploaded)
            file_mod = datetime.strptime(file_attr["modify"], "%Y%m%d%H%M%S")

            # File version
            file_version = re.match(r"(RA\d{6})\.7z", file_name)[1]

            self.files.append({
                "ver": file_version,
                "mod": file_mod,
                "start": file_start,
                "end": file_end
            })

        # Create GTFS only for maxfiles feeds
        self.files = self.files[:maxfiles]
        self.files[-1]["end"] = date.max

    def sync_feeds(self, shapes=False):

        # Remove unwanted feeds
        print("\033[1A\033[K" + "Removing excess files from feeds/ directory")

        if not os.path.exists("feeds"): os.mkdir("feeds")

        missing_versions = [i["ver"] for i in self.files]

        for file in os.scandir("feeds"):
            file_version = re.match(r"(RA\d{6})\.zip", file.name)[1]
            match_in_files = [i for i in self.files if i["ver"] == file_version]

            # Remove not required files
            if not match_in_files:
                os.remove(os.path.join("feeds", file.name))

            # Remove files created before ZTM file was uploaded/updated
            elif match_in_files[0]["mod"] > datetime.fromtimestamp(file.stat().st_mtime):
                os.remove(os.path.join("feeds", file.name))

            else:
                # If we want to generate shapes, but the file doesn't have shapes.txt we have to recreate it
                if shapes:
                    with zipfile.ZipFile(os.path.join("feeds", file.name), mode="r") as arch:
                        if shapes and "shapes.txt" not in arch.namelist():
                            file_is_missing_shapes = True
                        else:
                            file_is_missing_shapes = False
                else:
                    file_is_missing_shapes = False

                if file_is_missing_shapes:
                    os.remove(os.path.join("feeds", file.name))

                # If we don't remove the file, we don't need to recreate it
                else:
                    missing_versions.remove(file_version)

        # If there are files we need to create
        if missing_versions:
            self.updated = True

            print("\n" + "\033[2A\033[K" + "Found {} missing files - those files will be created".format(len(missing_versions)), end="\n\n")

            print("\033[1A\033[K" + "Creating shape generator")
            parser_shapes = Shaper() if shapes else False

            for idx, version in enumerate(missing_versions):
                print("\033[2A\033[K" + "Creating GTFS for missing version: {} (file {}/{})".format(version, idx+1, len(missing_versions)), end="\n\n")
                print("\033[1A\033[K" + "Calling Converter.create()")

                Converter.create(
                    version=version,
                    shapes=parser_shapes,
                    metro=False,
                    targetfile=os.path.join("feeds", version+".zip"),
                    clear_shape_errors=False
                )

            print("\033[2A\033[K" + "All missing files created")

    def merge(self, shapes=False):
        clear_directory("gtfs")

        self.routes = {}

        self.stops = {}
        self.stop_conversion = {} # (version, stop_id): merged_stop_id if merged_stop_id != stop_id

        # Create files which will be copied line-by-line
        for filename, headers in FILES_TO_COPY.items():
            if (not shapes) and filename == "shapes.txt": continue
            with open(os.path.join("gtfs", filename), mode="w", encoding="utf8", newline="\r\n") as f:
                f.write(",".join(headers) + "\n")

        # Read feeds
        for feed in self.files:

            print("\033[1A\033[K" + "Merging version {}".format(feed["ver"]))

            arch = zipfile.ZipFile(os.path.join("feeds", feed["ver"]+".zip"), mode="r")

            active_services = set()
            active_shapes = set()

            ### STOPS ###
            print("\033[1A\033[K" + "Merging version {}: stops.txt".format(feed["ver"]))
            with arch.open("stops.txt") as buff:
                for row in csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline="")):

                    # If it's the first time we see this stop_id, just save it and continue
                    if row["stop_id"] not in self.stops:
                        self.stops[row["stop_id"]] = row
                        continue

                    # List all stops with same original stop_id
                    # If any of them is closer then 10 meters to the one we're looking at, we'll say it's the same
                    # This also kinda assumes that all stop attributes are the same
                    similar_stops = [(i, j) for (i, j) in self.stops.items() if j["stop_id"] == row["stop_id"]]
                    for similar_stop_id, similar_stop_data in similar_stops:
                        distance = haversine(
                            (float(row["stop_lat"]), float(row["stop_lon"])),
                            (float(similar_stop_data["stop_lat"]), float(similar_stop_data["stop_lon"]))
                        )

                        if distance <= 0.01:
                            self.stop_conversion[(feed["ver"], row["stop_id"])] = similar_stop_id
                            break

                    # If there's no stop closer then 10m with the same original stop_id, just create a new entry
                    else:
                        # Get a unused suffix for stop_id
                        stop_id_suffix = 1
                        while row["stop_id"] + "/" + str(stop_id_suffix) in self.stops:
                            stop_id_suffix += 1

                        # Save the stop under a different id
                        stop_id = row["stop_id"] + "/" + str(stop_id_suffix)
                        self.stops[stop_id] = row
                        self.stop_conversion[(feed["ver"], row["stop_id"])] = stop_id

            ### ROUTES ###
            print("\033[1A\033[K" + "Merging version {}: routes.txt".format(feed["ver"]))
            with arch.open("routes.txt") as buff:
                for row in csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline="")):
                    if row["route_id"] not in self.routes:
                        self.routes[row["route_id"]] = row

            ### CALENDARS ###
            print("\033[1A\033[K" + "Merging version {}: calendar_dates.txt".format(feed["ver"]))

            file = open("gtfs/calendar_dates.txt", mode="a", encoding="utf8", newline="")
            writer = csv.DictWriter(file, FILES_TO_COPY["calendar_dates.txt"], extrasaction="ignore")

            with arch.open("calendar_dates.txt") as buff:
                for row in csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline="")):

                    active_date = datetime.strptime(row["date"], "%Y%m%d").date()

                    if feed["start"] <= active_date <= feed["end"]:
                        active_services.add(row["service_id"])

                        writer.writerow({
                            "date": row["date"],
                            "service_id": feed["ver"] + "/" + row["service_id"],
                            "exception_type": row["exception_type"]
                        })

            file.close()

            ### TRIPS ###
            print("\033[1A\033[K" + "Merging version {}: trips.txt".format(feed["ver"]))

            file = open("gtfs/trips.txt", mode="a", encoding="utf8", newline="")
            writer = csv.DictWriter(file, FILES_TO_COPY["trips.txt"], extrasaction="ignore")

            with arch.open("trips.txt") as buff:
                for row in csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline="")):

                    # Ignore trips which service is not active in version's effective range
                    if row["service_id"] not in active_services:
                        continue

                    if shapes:
                        active_shapes.add(row["shape_id"])
                        row["shape_id"] = feed["ver"] + "/" + row["shape_id"]

                    else:
                        row["shape_id"] = ""

                    row["service_id"] = feed["ver"] + "/" + row["service_id"]
                    row["trip_id"] = feed["ver"] + "/" + row["trip_id"]

                    writer.writerow(row)

            file.close()

            ### TIMES ###
            print("\033[1A\033[K" + "Merging version {}: stop_times.txt".format(feed["ver"]))

            file = open("gtfs/stop_times.txt", mode="a", encoding="utf8", newline="")
            writer = csv.DictWriter(file, FILES_TO_COPY["stop_times.txt"], extrasaction="ignore")

            with arch.open("stop_times.txt") as buff:
                for row in csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline="")):

                    # Ignore trips which service is not active in version's effective range
                    if row["trip_id"].split("/")[2] not in active_services:
                        continue

                    row["trip_id"] = feed["ver"] + "/" + row["trip_id"]
                    row["stop_id"] = self.stop_conversion.get((feed["ver"], row["stop_id"]), row["stop_id"])

                    if not shapes:
                        row["shape_dist_traveled"] = ""

                    writer.writerow(row)

            file.close()

            ### SHAPES ###
            if shapes:
                print("\033[1A\033[K" + "Merging version {}: shapes.txt".format(feed["ver"]))

                file = open("gtfs/shapes.txt", mode="a", encoding="utf8", newline="")
                writer = csv.DictWriter(file, FILES_TO_COPY["shapes.txt"], extrasaction="ignore")

                with arch.open("shapes.txt") as buff:
                    for row in csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline="")):

                        if row["shape_id"] not in active_shapes:
                            continue

                        row["shape_id"] = feed["ver"] + "/" + row["shape_id"]
                        writer.writerow(row)

                file.close()

            arch.close()

    def create_routes(self):
        # Open file
        file = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(file, ["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color", "route_sort_order"], extrasaction="ignore")
        writer.writeheader()

        # Divide routes into tram, bus and train for sorting
        tram_route_order = []
        bus_route_order = []
        train_route_order = []

        for i, j in self.routes.items():
            if j["route_type"] == "0": tram_route_order.append(i)
            elif j["route_type"] == "3": bus_route_order.append(i)
            elif j["route_type"] == "2": train_route_order.append(i)

        # Setting route order
        tram_route_order = sorted(tram_route_order, key=lambda i: i.ljust(2, "0") if i.isnumeric() else i)
        bus_route_order = sorted(bus_route_order, key=lambda i: i.replace("-", "0").ljust(3, "0"))
        train_route_order = sorted(train_route_order)

        sort_order = 3 # 1 and 2 is reserved for M1 and M2

        # Export routes to GTFS
        for route_id in chain(tram_route_order, bus_route_order, train_route_order):
            self.routes[route_id]["route_sort_order"] = str(sort_order)
            self.routes[route_id]["agency_id"] = "0"
            writer.writerow(self.routes[route_id])
            sort_order += 1

        file.close()

    def create_stops(self):
        # Open file
        file = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(file, ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "stop_IBNR", "platform_code", "wheelchair_boarding"], extrasaction="ignore")
        writer.writeheader()

        # Export stops to GTFS
        for stop_id in sorted(self.stops.keys()):
            self.stops[stop_id]["stop_id"] = stop_id
            writer.writerow(self.stops[stop_id])

        file.close()

    @classmethod
    def create(cls, maxfiles=10, shapes=False, metro=True, targetfile="gtfs.zip", remerge=True, reparse=False):

        print("Acquiring list of required files")
        self = cls()
        self.list_feeds(maxfiles)

        if reparse:
            print("\033[1A\033[K" + "Clearing local files")
            clear_directory("feeds")

        print("\033[1A\033[K" + "Updating local files")
        download_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.sync_feeds(shapes)

        if self.updated == True or remerge:
            print("\033[1A\033[K" + "Merging feeds")
            self.merge(shapes)

            print("\033[1A\033[K" + "Outputing merged routes")
            self.create_routes()

            print("\033[1A\033[K" + "Outputing merged stops")
            self.create_stops()

            print("\033[1A\033[K" + "Creating static files")
            version = "/".join([i["ver"] for i in self.files])
            Converter.static_files(shapes, version, download_time)

            if metro:
                print("\033[1A\033[K" + "Adding metro")
                Metro.add()

            print("\033[1A\033[K" + "Compressing")
            Converter.compress(targetfile)

        else:
            print("\033[1A\033[K" + "No new files found, no GTFS was created!")
            version = None

        return version
