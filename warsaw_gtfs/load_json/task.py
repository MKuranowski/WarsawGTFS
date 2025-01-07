from collections.abc import Iterable
from typing import Any

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Stop

from .stops import parse_stops


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
        data = r.resources["slowniki.json"].json()
        with r.db.transaction():
            self.load_stops(r.db, data)
        self.load_routes(r.db, data["linie"], data["typy_linii"])
        self.load_calendars(r.db, data["kalendarz"], data["typy_dni"])

    def load_stops(self, db: DBConnection, data: Any) -> None:
        db.create_many(Stop, parse_stops(data))

    def load_routes(self, db: DBConnection, routes: Any, route_types: Any) -> None:
        raise NotImplementedError

    def load_calendars(self, db: DBConnection, days: Any, day_types: Any) -> None:
        raise NotImplementedError

    def load_schedules(self, r: TaskRuntime) -> None:
        raise NotImplementedError

    def check_for_duplicate_stops(self, stops: Iterable[Stop]) -> None:
        seen = dict[str, Stop]()
        for stop in stops:
            if other := seen.get(stop.id):
                self.logger.warning("duplicate stop %s: %s & %s", stop.id, stop, other)
            else:
                seen[stop.id] = stop
