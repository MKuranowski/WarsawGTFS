
from typing import Dict, IO, List, Literal, Set
from datetime import date, timedelta
from logging import getLogger
import csv
import os

from ..const import HEADERS, DIR_SINGLE_FEED
from ..downloader import FileInfo
from ..fares import add_fare_info
from ..metro import append_metro_schedule
from ..util import clear_directory, compress, ensure_dir_exists, prepare_tempdir
from ..parser import Parser
from ..parser.dataobj import ZTMVariantStop

from .helpers import DirStopsType, FileNamespace, get_proper_headsign, get_route_color_type, \
    get_trip_direction, match_day_type
from .static_files import static_all
from .stophandler import StopHandler


class Converter:
    def __init__(self, version: str, parser: Parser, target_dir: str, start_date: date):
        """(Partially) inits the Converter.
        Since __init__ can't be asynchronous, caller has to also invoke converter.open_files():
        Make sure target_dir is clean before starting the converter.

        Also, await converter.close_files() after the converter finishes.
        """
        self.logger = getLogger(f"WarsawGTFS.{version}.Converter")

        # Data-related properties
        self.calendar_start = start_date - timedelta(days=30)
        self.calendars: Dict[date, List[str]] = {}
        self.routes: List[str] = []
        self.stops = StopHandler(version)

        # File-related properites
        self.target_dir = target_dir
        self.version = version
        self.parser = parser

        # It's the programmers responsiblity to call open_files and close_files
        # to ensure those variables are available during conversion
        self.file: "FileNamespace[IO[str]]"
        self.wrtr: "FileNamespace[csv._writer]"

        # Route-specific variables
        self.route_id: str = ""
        self.route_type: str = ""
        self.route_name: str
        self.direction_stops: DirStopsType
        self.inaccesible_trips: Set[str]
        self.on_demand_stops: Set[str]
        self.used_day_types: Set[str]
        self.variant_direction: Dict[str, Literal["0", "1"]]

    # File handlers

    def open_files(self):
        """Open file handlers used when converting"""
        def get_file_obj(name: str) -> IO[str]:
            return open(os.path.join(self.target_dir, name), mode="w", encoding="utf8", newline="")

        def get_writer(name: str, fileobj: IO[str]) -> "csv._writer":
            wrtr = csv.writer(fileobj)
            wrtr.writerow(HEADERS[name])
            return wrtr

        # Open files
        self.file = FileNamespace(
            get_file_obj("routes.txt"),
            get_file_obj("trips.txt"),
            get_file_obj("stop_times.txt"),
            get_file_obj("calendar_dates.txt"),
        )

        # Create CSV writers
        self.wrtr = FileNamespace(
            get_writer("routes.txt", self.file.routes),
            get_writer("trips.txt", self.file.trips),
            get_writer("stop_times.txt", self.file.times),
            get_writer("calendar_dates.txt", self.file.dates),
        )

    def close_files(self):
        """Close file handlers opened by open_files"""
        for i in self.file:
            i.close()

    # Stop & calendar loaders

    def get_calendars(self):
        """Loads info about calendars. Exhausts self.parser.parse_ka."""
        self.logger.info("Loading calendars (KA)")

        for day in self.parser.parse_ka():
            if day.date < self.calendar_start:
                continue

            self.calendars[day.date] = day.services

    def get_stops(self):
        """Loads info about calendars. Exhausts self.parser.parse_zp."""
        self.logger.info("Loading stops (ZP)")

        for group in self.parser.parse_zp():
            stops = [i for i in self.parser.parse_pr()]
            self.stops.load_group(group, stops)

    # Route data converters

    def _reset_route_vars(self):
        """Resets per-route variables"""
        self.route_name = ""
        self.direction_stops = {"0": set(), "1": set()}
        self.inaccesible_trips = set()
        self.on_demand_stops = set()
        self.used_day_types = set()
        self.variant_direction = {}

    def _set_route_name(self, variant_stops: List[ZTMVariantStop]):
        """Sets current route_long_name based on the list of stops of the main variant."""
        # Get group ids
        first_stop = variant_stops[0].id[:4]
        last_stop = variant_stops[-1].id[:4]

        # Get group names
        first_name = self.stops.names.get(first_stop)
        last_name = self.stops.names.get(last_stop)

        # Ensure both names are defined
        if first_name is None:
            raise KeyError(f"Missing name for stop group {first_stop!r}")

        if last_name is None:
            raise KeyError(f"Missing name for stop group {last_stop!r}")

        # Set route_name
        self.route_name = first_name + " — " + last_name

    def _get_variants(self):
        """Loads data about variants of a route. Exhausts self.parse.parse_tr."""
        self.logger.debug(f"Parsing schedules (TR) - {self.route_id}")

        for variant in self.parser.parse_tr():
            # Loads stops from LW sections
            variant_stops = [i for i in self.parser.parse_lw()]

            # Add zone info
            for stop in variant_stops:
                self.stops.zone_set(stop.id[:4], stop.zone)

            # Set direction_id for this variant
            self.variant_direction[variant.id] = variant.direction

            # Set the route_name: it should be the name of termins of first variant
            if not self.route_name:
                self._set_route_name(variant_stops)

            # Add on-demand stops from this variant
            self.on_demand_stops.update(i.id for i in variant_stops if i.on_demand)

            # Add stops to per-direction dict for automatic direction_id detection
            self.direction_stops[variant.direction].update(i.id for i in variant_stops)

            # Parse ODWG section parse to retrieve inaccessible trips
            # Only for trams: all busses & trains are accessible
            if self.route_type == "0":
                self.logger.debug(f"Parsing schedules (WG/OD) - {self.route_id}")
                for trip in self.parser.parse_wgod(self.route_type, self.route_id):
                    if not trip.accessible:
                        self.inaccesible_trips.add(trip.trip_id)
            else:
                self.parser.skip_to_section("RP", end=True)

    def _save_trips(self):
        """Dumps data from WK section to the GTFS. Exhausts self.parser.parse_wk."""
        self.logger.debug(f"Parsing schedules (WK) - {self.route_id}")

        for trip in self.parser.parse_wk(self.route_id):
            # Change stop_ids
            for stopt in trip.stops:
                stopt.stop = self.stops.get_id(stopt.stop)

            # Remove stoptimes with invalid stops
            trip.stops = [i for i in trip.stops if i.stop is not None]

            # Ignore trips with only one stoptime
            if len(trip.stops) <= 1:
                continue

            # Unpack data from trip_id
            trip_id_split = trip.id.split("/")
            variant_id = trip_id_split[1]
            day_type = trip_id_split[2]

            service_id = self.route_id + "/" + day_type
            del trip_id_split

            # Set exceptional trips
            exceptional = "0" if variant_id.startswith("TP-") or variant_id.startswith("TO-") \
                else "1"

            # Wheelchair accessibility
            wheelchair = "2" if trip.id in self.inaccesible_trips else "1"

            # Direction
            if variant_id in self.variant_direction:
                direction = self.variant_direction[variant_id]
            else:
                direction = get_trip_direction(
                    {i.original_stop for i in trip.stops},
                    self.direction_stops,
                )

                self.variant_direction[variant_id] = direction

            # Headsign
            last_stop: str = trip.stops[-1].stop  # type: ignore | checked earlier
            last_stop_name = self.stops.names.get(last_stop[:4], "")
            headsign = get_proper_headsign(last_stop, last_stop_name)

            del last_stop, last_stop_name
            if not headsign:
                self.logger.warn(f"No headsign for trip {trip.id}")

            # Mark day_type as used
            self.used_day_types.add(day_type)

            # Write to trips.txt
            self.wrtr.trips.writerow([
                self.route_id,
                service_id,
                trip.id,
                headsign,
                direction,
                "",
                exceptional,
                wheelchair,
                "1",
            ])

            # Convert stoptimes
            max_seq = len(trip.stops) - 1
            for seq, stopt in enumerate(trip.stops):
                # Pickup Type
                if seq == max_seq:
                    pickup = "1"
                elif "P" in stopt.flags:
                    pickup = "1"
                elif stopt.original_stop in self.on_demand_stops:
                    pickup = "3"
                else:
                    pickup = "0"

                # Drop-Off Type
                if seq == 0:
                    dropoff = "1"
                elif stopt.original_stop in self.on_demand_stops:
                    dropoff = "3"
                else:
                    dropoff = "0"

                # Mark stop as used
                self.stops.use(stopt.stop)  # type: ignore | ensured stopt.stop is str earlier

                # Write to stop_times.txt
                self.wrtr.times.writerow([
                    trip.id,
                    stopt.time,
                    stopt.time,
                    stopt.stop,
                    seq,
                    pickup,
                    dropoff,
                    "",
                ])

    def save_schedules(self):
        """Convert schedules into GTFS. Exhausts self.parser.parse_ll."""
        route_sort_order = 1  # First 2 are reserved for M1 and M2

        self.logger.info("Parsing schedules (LL)")

        for route in self.parser.parse_ll():
            self._reset_route_vars()
            self.route_id = route.id

            # Ignore Koleje Mazowieckie & Warszawska Kolej Dojazdowa routes
            # if self.route_id not in {"1", "9", "115", "525"}:
            if self.route_id.startswith("R") or self.route_id.startswith("WKD"):
                self.parser.skip_to_section("WK", end=True)
                continue

            self.routes.append(self.route_id)
            self.logger.debug(f"Parsing schedules (LL) - {self.route_id}")

            # Extarct basic route data
            route_sort_order += 1
            self.route_type, route_color, route_txt_color = get_route_color_type(
                self.route_id, route.desc)

            # Parse data from section TR
            self._get_variants()

            # Save trips to trips.txt and stop_times.txt
            self._save_trips()

            # Write to calendar_dates.txt
            for day, possible_day_types in self.calendars.items():
                active_day_type = match_day_type(self.used_day_types, possible_day_types)
                if active_day_type:
                    self.wrtr.dates.writerow([
                        self.route_id + "/" + active_day_type,
                        day.strftime("%Y%m%d"),
                        "1",
                    ])

            # Write to routes.txt
            self.wrtr.routes.writerow([
                "0",
                self.route_id,
                self.route_id,
                self.route_name,
                self.route_type,
                route_color,
                route_txt_color,
                route_sort_order,
            ])

    def convert(self):
        """Exhause whole self.parser.parse_* and export stops.txt"""
        self.get_calendars()
        self.get_stops()
        self.save_schedules()
        self.stops.export(self.target_dir)

    @classmethod
    def create(cls, finfo: FileInfo, target: str, sync_time: str, in_temp_dir: bool = False,
               pub_name: str = "", pub_url: str = "", metro: bool = False):
        # Open the ZTM txt file and wrap a Parser around it
        with open(finfo.path, mode="r", encoding="windows-1250") as f:
            parser = Parser(f, finfo.version)

            # Make the directory for the gtfs files
            if in_temp_dir:
                target_dir = prepare_tempdir(finfo.version)
            else:
                target_dir = DIR_SINGLE_FEED
                ensure_dir_exists(target_dir, clear=True)

            # Create Converter instance
            self = cls(finfo.version, parser, target_dir, finfo.start)
            self.open_files()

            # Parse data from ZTM file
            try:
                self.logger.info("Starting parser")
                self.convert()
            finally:
                self.close_files()

            self.logger.info("Parsing finished")

        self.logger.info("Creating static files")
        static_all(target_dir, False, finfo.version, sync_time, pub_name, pub_url)

        if metro:
            self.logger.info("Appending metro schedules")
            metro_routes = append_metro_schedule(target_dir)
            self.routes = metro_routes + self.routes

        self.logger.info("Adding fare info")
        add_fare_info(target_dir, self.routes)

        self.logger.info(f"Compressing to {target!r}")
        compress(target_dir, target)

        # Remove the tempdir after working with it
        if in_temp_dir:
            clear_directory(target_dir)
            os.rmdir(target_dir)
