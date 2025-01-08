from typing import Any, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, CalendarException, Route, Stop, StopTime, Trip

from .calendars import parse_calendar_exceptions, parse_calendars
from .routes import parse_routes
from .stop_times import parse_stop_times
from .stops import parse_stops
from .trips import parse_trips
from .variants import parse_variant_stops, parse_variants

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
        self.valid_stops = set[str]()

    def clear(self) -> None:
        self.valid_stops.clear()

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

    def load_stops(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stops")
        db.create_many(Stop, parse_stops(data))
        self.valid_stops = {
            cast(str, i[0]) for i in db.raw_execute("SELECT stop_id FROM stops", ())
        }

    def load_routes(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading routes")
        db.create_many(Route, parse_routes(data))

    def load_calendars(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading calendars")
        db.create_many(Calendar, parse_calendars(data))
        db.create_many(CalendarException, parse_calendar_exceptions(data))

    def load_schedules(self, r: TaskRuntime) -> None:
        self.logger.info("Loading schedules")
        data = r.resources["rozklady.json"].json()  # FIXME: Don't load the entire JSON to memory
        with r.db.transaction():
            self.load_variants(r.db, data)  # "warianty"
            self.load_variant_stops(r.db, data)  # "odcinki"
            # self.load_shapes()  #  "ksztalt_trasy_GPS"
            self.load_trips(r.db, data)  # "rozklady_jazdy"
            self.load_stop_times(r.db, data)  # "kursy_przejazdy"

    def load_variants(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading variants")
        db.raw_execute_many("INSERT INTO variants VALUES (?,?,?,?,?,?)", parse_variants(data))

    def load_variant_stops(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading variant stops")
        db.raw_execute_many(
            "INSERT INTO variant_stops VALUES (?,?,?,?,?,?,?)",
            (i for i in parse_variant_stops(data) if i[2] in self.valid_stops),
        )

    def load_trips(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading trips")
        db.create_many(Trip, parse_trips(data))

    def load_stop_times(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stop times")
        db.create_many(
            StopTime,
            (i for i in parse_stop_times(data) if i.stop_id in self.valid_stops),
        )
