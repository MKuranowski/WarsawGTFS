import json
from collections.abc import Mapping
from typing import Any
from zipfile import ZipFile

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import CalendarException, StopTime
from impuls.resource import ManagedResource

from .calendars import parse_calendar_exceptions, parse_calendars
from .routes import parse_routes
from .shapes import parse_shape_points, parse_shapes
from .stop_times import parse_stop_times
from .stops import parse_stops
from .trips import parse_trips
from .variants import parse_variant_stops, parse_variants
from .vehicles import VehicleKind, parse_vehicle_kinds

DB_SCHEMA_EXTENSION = """
CREATE TABLE variants (
    variant_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE ON UPDATE CASCADE,
    code TEXT NOT NULL,
    direction INTEGER,
    is_main INTEGER NOT NULL DEFAULT 0 CHECK (is_main IN (0, 1)),
    is_exceptional INTEGER NOT NULL DEFAULT 0 CHECK (is_exceptional IN (0, 1))
) STRICT;

CREATE TABLE variant_stops (
    variant_id TEXT NOT NULL REFERENCES variants(variant_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL REFERENCES stops(stop_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    is_request INTEGER NOT NULL DEFAULT 0 CHECK (is_request IN (0, 1)),
    is_not_available INTEGER NOT NULL DEFAULT 0 CHECK (is_not_available IN (0, 1)),
    is_virtual INTEGER NOT NULL DEFAULT 0 CHECK (is_virtual IN (0, 1)),
    accessibility INTEGER,
    PRIMARY KEY (variant_id, stop_sequence)
) STRICT;
"""


class LoadJSON(Task):
    def __init__(self, resource_name: str) -> None:
        super().__init__()
        self.resource_name = resource_name
        self.vehicle_kinds = dict[int, VehicleKind]()
        self.route_id_lookup = dict[int, str]()
        self.stop_id_lookup = dict[int, str]()
        self.calendar_id_lookup = dict[int, str]()
        self.trip_id_lookup = dict[int, str]()

    def clear(self) -> None:
        self.vehicle_kinds.clear()
        self.route_id_lookup.clear()
        self.stop_id_lookup.clear()
        self.calendar_id_lookup.clear()
        self.trip_id_lookup.clear()

    def execute(self, r: TaskRuntime) -> None:
        self.clear()
        self.extend_schema(r.db)
        self.load_lookup_tables(r)
        self.load_schedules(r)

    @staticmethod
    def extend_schema(db: DBConnection) -> None:
        db._con.executescript(DB_SCHEMA_EXTENSION)  # type: ignore

    def load_lookup_tables(self, r: TaskRuntime) -> None:
        self.logger.info("Loading lookup tables")
        data = self.load_json(r.resources, "slowniki.json")
        with r.db.transaction():
            self.load_stops(r.db, data)
            self.load_routes(r.db, data)
            self.load_calendars(r.db, data)
            self.load_vehicle_kinds(data)

    def load_stops(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stops")
        for original_id, stop in parse_stops(data):
            self.stop_id_lookup[original_id] = stop.id
            db.create(stop)

    def load_routes(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading routes")
        for original_id, route in parse_routes(data):
            self.route_id_lookup[original_id] = route.id
            db.create(route)

    def load_calendars(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading calendars")
        for original_id, calendar in parse_calendars(data):
            self.calendar_id_lookup[original_id] = calendar.id
            db.create(calendar)
        db.create_many(CalendarException, parse_calendar_exceptions(data, self.calendar_id_lookup))

    def load_vehicle_kinds(self, data: Any) -> None:
        self.logger.debug("Loading vehicle kinds")
        self.vehicle_kinds = parse_vehicle_kinds(data)

    def load_schedules(self, r: TaskRuntime) -> None:
        self.logger.info("Loading schedules")
        # TODO: Don't load the entire JSON to memory
        data = self.load_json(r.resources, "rozklady.json")
        with r.db.transaction():
            self.load_variants(r.db, data)
            self.load_variant_stops(r.db, data)
            self.load_shapes(r.db, data)
            self.load_trips(r.db, data)
            self.load_stop_times(r.db, data)

    def load_variants(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading variants")
        assert self.route_id_lookup
        db.raw_execute_many(
            "INSERT INTO variants VALUES (?,?,?,?,?,?)",
            parse_variants(data, self.route_id_lookup),
        )

    def load_variant_stops(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading variant stops")
        assert self.stop_id_lookup
        db.raw_execute_many(
            "INSERT INTO variant_stops VALUES (?,?,?,?,?,?,?)",
            parse_variant_stops(data, self.stop_id_lookup),
        )

    def load_trips(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading trips")
        assert self.route_id_lookup
        assert self.calendar_id_lookup
        trips = parse_trips(data, self.route_id_lookup, self.calendar_id_lookup, self.vehicle_kinds)
        for original_id, trip in trips:
            self.trip_id_lookup[original_id] = trip.id
            db.create(trip)

    def load_stop_times(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stop times")
        assert self.trip_id_lookup
        assert self.stop_id_lookup
        db.create_many(StopTime, parse_stop_times(data, self.trip_id_lookup, self.stop_id_lookup))

    def load_shapes(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading shapes")
        db.raw_execute_many("INSERT INTO shapes (shape_id) VALUES (?)", parse_shapes(data))
        db.raw_execute_many(
            "INSERT INTO shape_points (shape_id, sequence, lat, lon) VALUES (?,?,?,?)",
            parse_shape_points(data),
        )

    def load_json(self, resources: Mapping[str, ManagedResource], name: str) -> Any:
        with ZipFile(resources[self.resource_name].stored_at, mode="r") as arch:
            with arch.open(name, mode="r") as f:
                return json.load(f)
