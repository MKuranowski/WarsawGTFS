from collections.abc import Container
from typing import LiteralString, cast

from impuls import DBConnection, Task, TaskRuntime


class StabilizeIds(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            self.stabilize_route_ids(r.db)
            self.stabilize_stop_ids(r.db)
            self.stabilize_calendar_ids(r.db)
            self.stabilize_trip_ids(r.db)

    def stabilize_route_ids(self, db: DBConnection) -> None:
        self.stabilize_ids(db, "routes", "route_id", "SELECT route_id, short_name FROM routes")

    def stabilize_stop_ids(self, db: DBConnection) -> None:
        self.stabilize_ids(db, "stops", "stop_id", "SELECT stop_id, code FROM stops")

    def stabilize_calendar_ids(self, db: DBConnection) -> None:
        self.stabilize_ids(
            db,
            "calendars",
            "calendar_id",
            "SELECT calendar_id, desc FROM calendars",
        )

    def stabilize_trip_ids(self, db: DBConnection) -> None:
        self.stabilize_ids(
            db,
            "trips",
            "trip_id",
            (
                "SELECT t.trip_id, r.short_name, c.desc, t.extra_fields_json ->> 'brigade', "
                "  (SELECT format('%02d%02d', st.departure_time/3600, st.departure_time/60%60) "
                "   FROM stop_times st WHERE t.trip_id = st.trip_id "
                "   ORDER BY st.stop_sequence ASC LIMIT 1) "
                "FROM trips t "
                "LEFT JOIN routes r ON (t.route_id = r.route_id) "
                "LEFT JOIN calendars c ON (t.calendar_id = c.calendar_id)"
            ),
            may_collide=False,
        )

    def stabilize_ids(
        self,
        db: DBConnection,
        table: LiteralString,
        key: LiteralString,
        select: LiteralString,
        may_collide: bool = True,
    ) -> None:
        self.logger.info("Stabilizing %ss", key)

        if may_collide:
            self.logger.debug("Tentatively renaming %ss to prevent collisions", key)
            db.raw_execute(f"UPDATE {table} SET {key} = '_old_' || {key}")

        self.logger.debug("Generating new %ss", key)
        changes = self.get_id_changes(db, select)

        self.logger.debug("Applying new %ss ", key)
        db.raw_execute_many(f"UPDATE {table} SET {key} = ? WHERE {key} = ?", changes)

    @staticmethod
    def get_id_changes(db: DBConnection, select: LiteralString) -> list[tuple[str, str]]:
        changes = list[tuple[str, str]]()
        new_ids = set[str]()
        for row in db.raw_execute(select):
            old_id = cast(str, row[0])
            stem = ":".join(str(i) for i in row[1:])
            new_id = generate_unique_id(stem, new_ids)
            new_ids.add(new_id)
            changes.append((new_id, old_id))
        return changes


def generate_unique_id(stem: str, used_ids: Container[str]) -> str:
    """Generates a unique ID (not present in ``used_ids``) based on the provided
    ``stem``, by adding suffixes ``:1``, ``:2``, until a unique ID is found.
    If ``stem`` is not in ``used_ids`` it is returned directly.
    """
    i = 0
    id = stem
    while id in used_ids:
        i += 1
        id = f"{stem}:{i}"
    return id
