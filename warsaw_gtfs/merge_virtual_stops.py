import re
from collections.abc import Iterable
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

from .util import is_railway_stop


class MergeVirtualStops(Task):
    def __init__(self, explicit_virtual_stops: Iterable[str] = []) -> None:
        super().__init__()
        self.explicit_virtual_stops = list(explicit_virtual_stops)

    def execute(self, r: TaskRuntime) -> None:
        all = self.get_all_stop_ids(r.db)
        virtual = self.find_virtual_stops(all)
        with r.db.transaction():
            for new_id, old_id in self.generate_replacement_pairs(virtual, all):
                r.db.raw_execute(
                    "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
                    (new_id, old_id),
                )
                r.db.raw_execute("DELETE FROM stops WHERE stop_id = ?", (old_id,))

    @staticmethod
    def get_all_stop_ids(db: DBConnection) -> set[str]:
        return {cast(str, i[0]) for i in db.raw_execute("SELECT stop_id FROM stops")}

    def find_virtual_stops(self, all_ids: set[str]) -> set[str]:
        base = {i for i in all_ids if re.match(r"^[0-9]{4}8[1-9]$", i) and not is_railway_stop(i)}
        for id in self.explicit_virtual_stops:
            if id in all_ids:
                base.add(id)
            else:
                self.logger.warning("Won't explicitly merge %s - stop does not exist", id)
        return base

    def generate_replacement_pairs(
        self,
        virtual: set[str],
        all: set[str],
    ) -> Iterable[tuple[str, str]]:
        for id in virtual:
            if replacement := self.find_replacement_stop(id, all):
                self.logger.info("Merging %s into %s", id, replacement)
                yield replacement, id
            else:
                self.logger.warning("No replacement for virtual stop %s", id)

    @staticmethod
    def find_replacement_stop(virtual: str, all: set[str]) -> str | None:
        # Special case for Metro Młociny 88 - map to 28
        if virtual == "605988" and "605928" in all:
            return "605928"

        # Try to replace 8x by 0x, 1x, ..., 7x
        for i in range(8):
            candidate = f"{virtual[:4]}{i}{virtual[5:]}"
            if candidate != virtual and candidate in all:
                return candidate
