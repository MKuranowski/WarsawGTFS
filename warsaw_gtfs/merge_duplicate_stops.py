import math
from collections.abc import Iterable
from operator import itemgetter
from typing import NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Stop
from impuls.tools.geo import earth_distance_m

MAX_MERGE_DISTANCE_M = 50.0


class MergePair(NamedTuple):
    dst: str
    src: str


class MergeDuplicateStops(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        for code in self.find_duplicate_codes(r.db):
            self.merge_duplicates(r.db, code)

    def find_duplicate_codes(self, db: DBConnection) -> list[str]:
        return [
            cast(str, i[0])
            for i in db.raw_execute("SELECT code FROM stops GROUP BY code HAVING COUNT(*) > 1")
        ]

    def merge_duplicates(self, db: DBConnection, code: str) -> None:
        stops = list(db.typed_out_execute("SELECT * FROM stops WHERE code = ?", Stop, (code,)))
        duplicates = self.resolve_duplicates(stops)
        self.execute_merge(db, duplicates)

    def resolve_duplicates(self, stops: Iterable[Stop]) -> list[MergePair]:
        valid = list[Stop]()
        to_merge = list[MergePair]()

        for stop in stops:
            best, dist_m = self.find_closest_candidate(valid, stop)
            if best and dist_m < MAX_MERGE_DISTANCE_M:
                self.logger.warning("Merging %s into %s", stop, best)
                to_merge.append(MergePair(best.id, stop.id))
            else:
                valid.append(stop)

        return to_merge

    def find_closest_candidate(
        self,
        valid: Iterable[Stop],
        stop: Stop,
    ) -> tuple[Stop | None, float]:
        return min(
            (
                (candidate, earth_distance_m(candidate.lat, candidate.lon, stop.lat, stop.lon))
                for candidate in valid
            ),
            key=itemgetter(1),
            default=(None, math.inf),
        )

    def execute_merge(self, db: DBConnection, duplicates: list[MergePair]) -> None:
        db.raw_execute_many(
            "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
            ((i.dst, i.src) for i in duplicates),
        )
        db.raw_execute_many(
            "UPDATE variant_stops SET stop_id = ? WHERE stop_id = ?",
            ((i.dst, i.src) for i in duplicates),
        )
        db.raw_execute_many("DELETE FROM stops WHERE stop_id = ?", ((i.src,) for i in duplicates))
