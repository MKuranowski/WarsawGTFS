from collections.abc import Iterable, Mapping
from typing import cast

from impuls import DBConnection, Task, TaskRuntime


class AssignZoneId(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        zone_ids = self.assign_zone_ids_to_stop_groups(self.generate_zone_candidates(r.db))
        self.save_assigned_zone_ids(r.db, zone_ids)

    def generate_zone_candidates(self, db: DBConnection) -> Iterable[tuple[str, str]]:
        q = db.raw_execute("SELECT stop_id, zone_id, is_zone_border FROM variant_stops")
        for row in q:
            stop_id = cast(str, row[0])
            is_zone_border = cast(int, row[2])
            zone_id = "1+2" if is_zone_border else cast(str, row[1])
            yield stop_id, zone_id

    def assign_zone_ids_to_stop_groups(
        self,
        candidates: Iterable[tuple[str, str]],
    ) -> dict[str, str]:
        zone_ids = dict[str, str]()
        for stop_id, zone_id_candidate in candidates:
            group_id = stop_id[:4]
            if existing_zone_id := zone_ids.get(group_id):
                if existing_zone_id != zone_id_candidate:
                    if existing_zone_id != "1+2" and zone_id_candidate != "1+2":
                        self.logger.warning("Conflicting zone ids in group %s", group_id)
                    zone_ids[group_id] = "1+2"
            else:
                zone_ids[group_id] = zone_id_candidate

        return zone_ids

    def save_assigned_zone_ids(self, db: DBConnection, zone_ids: Mapping[str, str]) -> None:
        with db.transaction():
            db.raw_execute_many(
                "UPDATE stops SET zone_id = ? WHERE substr(stop_id, 1, 4) = ?",
                ((zone_id, group_id) for group_id, zone_id in zone_ids.items()),
            )
