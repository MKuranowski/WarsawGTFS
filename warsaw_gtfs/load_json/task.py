from typing import Any, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, CalendarException, Route, Stop, StopTime, Trip

from .calendars import parse_calendar_exceptions, parse_calendars
from .routes import parse_routes
from .stop_times import parse_stop_times
from .stops import parse_stops
from .trips import parse_trips


class LoadJSON(Task):
    def __init__(self) -> None:
        super().__init__()
        self.stop_id_mapping = dict[int, str]()

    def clear(self) -> None:
        self.stop_id_mapping.clear()

    def execute(self, r: TaskRuntime) -> None:
        self.clear()
        self.load_lookup_tables(r)
        self.load_schedules(r)

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

    def load_routes(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading routes")
        db.create_many(Route, parse_routes(data))

    def load_calendars(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading calendars")
        db.create_many(Calendar, parse_calendars(data))
        db.create_many(CalendarException, parse_calendar_exceptions(data))

    def load_schedules(self, r: TaskRuntime) -> None:
        self.logger.info("Loading schedules")
        data = r.resources["rozklady.json"].json()
        with r.db.transaction():
            # self.load_variants()  # "warianty"
            # self.load_variant_stops()  # "odcinki"
            # self.load_shapes()  #  "ksztalt_trasy_GPS"
            self.load_trips(r.db, data)  # "rozklady_jazdy"
            self.load_stop_times(r.db, data)  # "kursy_przejazdy"

    def load_trips(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading trips")
        db.create_many(Trip, parse_trips(data))

    def load_stop_times(self, db: DBConnection, data: Any) -> None:
        self.logger.debug("Loading stop times")
        valid_stops = {cast(str, i[0]) for i in db.raw_execute("SELECT stop_id FROM stops", ())}
        db.create_many(StopTime, (i for i in parse_stop_times(data) if i.stop_id in valid_stops))
