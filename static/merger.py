
import csv
import io
from contextlib import contextmanager
from datetime import datetime
from logging import getLogger
from operator import itemgetter
from os import rmdir
from os.path import join
from typing import IO, Dict, Iterable, List, Set, Tuple
from zipfile import ZipFile

from pyroutelib3 import distHaversine

from .const import DIR_SINGLE_FEED, HEADERS
from .converter.static_files import static_all
from .downloader import FileInfo
from .fares import add_fare_info
from .metro import append_metro_schedule
from .util import (ConversionOpts, clear_directory, compress,
                   ensure_dir_exists, prepare_tempdir)

"""
Module implements functionality to merge multiple converted GTFS feeds.
"""


class ZipFileWithCsv(ZipFile):
    @contextmanager
    def open_csv(self, fname: str):
        bin_buff = self.open(fname, mode="r")
        txt_buff = io.TextIOWrapper(bin_buff, encoding="utf-8", newline="")

        try:
            yield csv.DictReader(txt_buff)
        finally:
            txt_buff.close()
            bin_buff.close()


class Merger:

    # Merger initialization

    def __init__(self, files: List[FileInfo], target_dir: str, shapes: bool) -> None:
        self.files = files
        self.shapes = shapes
        self.target_dir = target_dir
        self.logger = getLogger("WarsawGTFS.Merger")

        # Data structures for holding merged data
        self.routes: Dict[str, Dict[str, str]] = {}
        self.stops: Dict[str, Dict[str, str]] = {}
        self.stop_conversion: Dict[Tuple[str, str], str] = {}

        # Per-file attributes
        self.file: FileInfo
        self.active_services: Set[str]
        self.active_trips: Set[str]
        self.active_shapes: Set[str]

        # Writers for files that are created incrementally
        self.file_calendar: IO[str]
        self.file_trips: IO[str]
        self.file_times: IO[str]
        self.file_shapes: IO[str]

        self.wrtr_calendar: csv.DictWriter
        self.wrtr_trips: csv.DictWriter
        self.wrtr_times: csv.DictWriter
        self.wrtr_shapes: csv.DictWriter

    def _clear_per_file_attrs(self, file: FileInfo) -> None:
        """Clears variables used per each merged feed"""
        self.file = file
        self.active_services = set()
        self.active_trips = set()
        self.active_shapes = set()

    def _open_incremental_files(self) -> None:
        """
        Open files handlers and creates csv writers for
        GTFS files that are written to incrementally.
        """
        def get_file_wrtr(fname: str) -> Tuple[IO[str], csv.DictWriter]:
            f = open(join(self.target_dir, fname), mode="w", encoding="utf-8", newline="")
            w = csv.DictWriter(f, HEADERS[fname])
            w.writeheader()
            return f, w

        self.file_calendar, self.wrtr_calendar = get_file_wrtr("calendar_dates.txt")
        self.file_trips, self.wrtr_trips = get_file_wrtr("trips.txt")
        self.file_times, self.wrtr_times = get_file_wrtr("stop_times.txt")

        if self.shapes:
            self.file_shapes, self.wrtr_shapes = get_file_wrtr("shapes.txt")
        else:
            self.file_shapes = None  # type: ignore
            self.wrtr_shapes = None  # type: ignore

    def _close_incremental_files(self) -> None:
        """Closes files opened by Merger._open_incremental_files()"""
        self.file_calendar.close()
        self.file_trips.close()
        self.file_times.close()
        if self.shapes:
            self.file_shapes.close()

    # Shortcuts for interacting with loaded data

    def _silimar_stops(self, stop_id: str) -> List[Dict[str, str]]:
        similar_stops = []
        suffix = 0

        while (similar_stop := self.stops.get(stop_id + (f"/{suffix}" if suffix else ""))):
            similar_stops.append(similar_stop)
            suffix += 1

        return similar_stops

    def _prepend_values_with_version(self, row: Dict[str, str], keys: Iterable[str]) -> None:
        for k in keys:
            row[k] = self.file.version + "/" + row[k]

    def _get_sorted_route_ids(self) -> List[str]:
        # Divide routes into tram, bus and train for sorting
        tram_routes = []
        bus_routes = []
        train_routes = []

        for i, j in self.routes.items():
            if j["route_type"] == "0":
                tram_routes.append(i)
            elif j["route_type"] == "3":
                bus_routes.append(i)
            elif j["route_type"] == "2":
                train_routes.append(i)

        tram_routes.sort(key=lambda i: i.rjust(2, "0") if i.isnumeric() else i)
        bus_routes.sort(key=lambda i: i.replace("-", "0").ljust(3, "0"))
        train_routes.sort()

        return tram_routes + bus_routes + train_routes

    # Per-feed operations

    def load_routes(self, reader: csv.DictReader) -> None:
        """Loads routes from a reader for later output"""
        self.logger.info(f"Merging {self.file.version}: routes.txt")
        for row in reader:
            if row["route_id"] not in self.routes:
                self.routes[row["route_id"]] = row

    def load_stops(self, reader: csv.DictReader) -> None:
        """Loads stops from reader for later output"""
        self.logger.info(f"Merging {self.file.version}: stops.txt")

        for row in reader:
            stop_id = row["stop_id"]

            # If it's the first time we see this stop_id, just save it and continue
            if stop_id not in self.stops:
                self.stops[stop_id] = row
                continue

            # List all stops with same stop_id
            # If any of them is closer than 10 meters and has the same name:
            # Consider those stops are the same.
            similar_stops = self._silimar_stops(stop_id)
            for similar_stop in similar_stops:
                # Extract some data about the similar stop
                similar_stop_id = similar_stop["stop_id"]
                similar_stop_suffix = similar_stop_id.split("/") if "/" in similar_stop_id else ""
                similar_stop_pos = float(similar_stop["stop_lat"]), float(similar_stop["stop_lon"])

                # Calculate the distance difference
                distance = distHaversine(
                    (float(row["stop_lat"]), float(row["stop_lon"])), similar_stop_pos
                )

                # Check if the similar stop is "close enough"
                if distance <= 0.01 and similar_stop["stop_name"] == row["stop_name"]:
                    # Only save to stop_conversion if the suffix is set
                    if similar_stop_suffix:
                        self.stop_conversion[self.file.version, stop_id] = similar_stop_id

                    break

            # If there's no stop "close-enough" - append it to known stops with a new suffix
            else:
                # Disallow new topologies for stops belonging to a group, as the
                # stop-station structure would be heavily screwed up
                if row["location_type"]:
                    raise ValueError(
                        f"Stop {stop_id} belongs to a stop-station group, and "
                        f"changes its position/name in feed {self.file.version}.")

                # Get a unused suffix for stop_id
                if len(similar_stops) <= 1:
                    new_suffix = 1
                else:
                    new_suffix = int(similar_stops[-1]["stop_id"].split("/")[1]) + 1

                # Save the stop under a different id
                stop_id = stop_id + "/" + str(new_suffix)
                row["stop_id"] = stop_id
                self.stops[stop_id] = row
                self.stop_conversion[(self.file.version, row["stop_id"])] = stop_id

    def merge_calendars(self, reader: csv.DictReader) -> None:
        """Incrementally merge rows from calendar_dates.txt"""
        self.logger.info(f"Merging {self.file.version}: calendar_dates.txt")

        for row in reader:
            day = datetime.strptime(row["date"], "%Y%m%d").date()
            if self.file.start <= day <= self.file.end:
                # Save outputted primary keys
                self.active_services.add(row["service_id"])

                # Prepend per-file ids
                self._prepend_values_with_version(row, {"service_id"})

                # Re-write the row
                self.wrtr_calendar.writerow(row)

    def merge_trips(self, reader: csv.DictReader) -> None:
        """Incrementally merge rows from trips.txt"""
        self.logger.info(f"Merging {self.file.version}: trips.txt")

        for row in reader:
            if row["service_id"] in self.active_services:
                # Save outputted primary keys and
                # determine which fields should be prepended with feed.version
                self.active_trips.add(row["trip_id"])
                prepend_keys = {"trip_id", "service_id"}

                if self.shapes:
                    # If shapes are expected to be in the result file -
                    # consider 'shape_id' fields for the above actions
                    self.active_shapes.add(row["shape_id"])
                    prepend_keys.add("shape_id")
                else:
                    # If no shapes - force-clear the shape_id field.
                    # This is to prevent invalid references if source files have shapes, but
                    # the shape option wasn't set in the Merger
                    row["shape_id"] = ""

                self._prepend_values_with_version(row, prepend_keys)

                # Re-write the row
                self.wrtr_trips.writerow(row)

    def merge_times(self, reader: csv.DictReader) -> None:
        """Incrementally merge rows from stop_times.txt"""
        self.logger.info(f"Merging {self.file.version}: stop_times.txt")

        for row in reader:
            if row["trip_id"] in self.active_trips:
                # Prepend per-file ids
                self._prepend_values_with_version(row, {"trip_id"})

                # Swap stop_id
                stop_conversion_key = self.file.version, row["stop_id"]
                row["stop_id"] = self.stop_conversion.get(stop_conversion_key, row["stop_id"])

                # If no shapes - force-clear the shape_dist_traveled field.
                # This is to prevent invalid references if source files have shapes, but
                # the shape option wasn't set in the Merger
                if not self.shapes:
                    row["shape_dist_traveled"] = ""

                # Re-write the row
                self.wrtr_times.writerow(row)

    def merge_shapes(self, reader: csv.DictReader) -> None:
        """Incrementally merge rows from shapes.txt"""
        self.logger.info(f"Merging {self.file.version}: shapes.txt")

        for row in reader:
            if row["shape_id"] in self.active_shapes:
                # Prepend per-file ids
                self._prepend_values_with_version(row, {"shape_id"})

                # Re-write the row
                self.wrtr_shapes.writerow(row)

    # Actual data merging

    def save_routes(self) -> List[str]:
        self.logger.info("Writing merged routes.txt")
        file = open(join(self.target_dir, "routes.txt"), mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, HEADERS["routes.txt"])
        writer.writeheader()
        all_routes = []

        # sort_orders '0' and '1' is used for M1 and M2
        for sort_order, route_id in enumerate(self._get_sorted_route_ids(), start=2):
            all_routes.append(route_id)
            row = self.routes[route_id]
            row["route_sort_order"] = str(sort_order)
            writer.writerow(row)

        file.close()
        return all_routes

    def save_stops(self) -> None:
        self.logger.info("Writing merged stops.txt")
        file = open(join(self.target_dir, "stops.txt"), mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, HEADERS["stops.txt"])
        writer.writeheader()

        for _, row in sorted(self.stops.items(), key=itemgetter(0)):
            writer.writerow(row)

        file.close()

    # Full create-merged-feed operation

    @classmethod
    def create(
            cls,
            files: List[FileInfo],
            opts: ConversionOpts,
            in_temp_dir: bool = False) -> None:
        """Merges all converted ZTM files to a single GTFS."""

        # Make the directory for the gtfs files
        if in_temp_dir:
            target_dir = prepare_tempdir("merged")
        else:
            target_dir = DIR_SINGLE_FEED
            ensure_dir_exists(target_dir, clear=True)

        # Initialize the merger
        self = cls(files, target_dir, opts.shapes)
        self._open_incremental_files()

        # Load per-file data
        feed_loaders = [
            ("routes.txt", self.load_routes),
            ("stops.txt", self.load_stops),
            ("calendar_dates.txt", self.merge_calendars),
            ("trips.txt", self.merge_trips),
            ("stop_times.txt", self.merge_times),
        ]

        if opts.shapes:
            feed_loaders.append(("shapes.txt", self.merge_shapes))

        for file in files:
            self._clear_per_file_attrs(file)
            arch = ZipFileWithCsv(file.path)

            for gtfs_fname, operation in feed_loaders:
                with arch.open_csv(gtfs_fname) as reader:
                    operation(reader)

        self._close_incremental_files()

        # Export routes and stops
        all_routes = self.save_routes()
        self.save_stops()

        # Create static files
        self.logger.info("Creating static files")
        static_all(target_dir, "/".join(i.version for i in files), opts)

        # Add metro schedules
        if opts.metro:
            self.logger.info("Appending metro schedules")
            metro_routes = append_metro_schedule(target_dir)
            all_routes += metro_routes

        # Add fare info
        self.logger.info("Adding fare info")
        add_fare_info(target_dir, all_routes)

        # Compress to .zip
        self.logger.info(f"Compressing to {opts.target!r}")
        compress(target_dir, opts.target)

        # Remove the tempdir after working with it
        if in_temp_dir:
            clear_directory(target_dir)
            rmdir(target_dir)
