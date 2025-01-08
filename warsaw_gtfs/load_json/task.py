from typing import Any

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, CalendarException, Route, Stop, StopTime, Trip

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
    stop_id TEXT NOT NULL REFERENCES stops(stop_id),
    is_request INTEGER NOT NULL DEFAULT 0 CHECK (is_request IN (0, 1)),
    is_not_available INTEGER NOT NULL DEFAULT 0 CHECK (is_not_available IN (0, 1)),
    is_virtual INTEGER NOT NULL DEFAULT 0 CHECK (is_virtual IN (0, 1)),
    accessibility INTEGER,
    PRIMARY KEY (variant_id, stop_sequence)
) STRICT;
"""


class LoadJSON(Task):
    def __init__(self) -> None:
        super().__init__()
        self.vehicle_kinds = dict[int, VehicleKind]()

    def clear(self) -> None:
        self.vehicle_kinds.clear()

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
        data = r.resources["slowniki.json"].json()
        with r.db.transaction():
            self.load_stops(r.db, data)
            self.load_routes(r.db, data)
            self.load_calendars(r.db, data)
            self.load_vehicle_kinds(data)

    def load_stops(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stops")
        db.create_many(Stop, parse_stops(data))

    def load_routes(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading routes")
        db.create_many(Route, parse_routes(data))

    def load_calendars(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading calendars")
        db.create_many(Calendar, parse_calendars(data))
        db.create_many(CalendarException, parse_calendar_exceptions(data))

    def load_vehicle_kinds(self, data: Any) -> None:
        self.logger.debug("Loading vehicle kinds")
        self.vehicle_kinds = parse_vehicle_kinds(data)

    def load_schedules(self, r: TaskRuntime) -> None:
        self.logger.info("Loading schedules")
        data = r.resources["rozklady.json"].json()  # FIXME: Don't load the entire JSON to memory
        with r.db.transaction():
            self.load_variants(r.db, data)
            self.load_variant_stops(r.db, data)
            self.load_shapes(r.db, data)
            self.load_trips(r.db, data)
            self.load_stop_times(r.db, data)

    def load_variants(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading variants")
        db.raw_execute_many("INSERT INTO variants VALUES (?,?,?,?,?,?)", parse_variants(data))

    def load_variant_stops(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading variant stops")
        db.raw_execute_many(
            "INSERT INTO variant_stops VALUES (?,?,?,?,?,?,?)",
            parse_variant_stops(data),
        )

    def load_trips(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading trips")
        db.create_many(Trip, parse_trips(data, self.vehicle_kinds))

    def load_stop_times(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stop times")
        db.create_many(StopTime, parse_stop_times(data))

    def load_shapes(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading shapes")
        db.raw_execute_many("INSERT INTO shapes (shape_id) VALUES (?)", parse_shapes(data))
        db.raw_execute_many(
            "INSERT INTO shape_points (shape_id, sequence, lat, lon) VALUES (?,?,?,?)",
            parse_shape_points(data),
        )
