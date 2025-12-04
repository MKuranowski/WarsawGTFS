from collections.abc import Iterable, Sequence
from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime

PaxExchange = StopTime.PassengerExchange


class CollapseDuplicateStopTimes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        self.logger.debug("Getting duplicate stop times")
        duplicates = self.find_duplicates(r.db)

        self.logger.debug("Solving duplicates")
        solutions = self.solve_duplicates(r.db, duplicates)

        self.logger.debug("Applying fixes")
        self.apply_solutions(r.db, solutions)

    def find_duplicates(self, db: DBConnection) -> list[tuple[str, int]]:
        return [
            (cast(str, r[0]), cast(int, r[1]))
            for r in db.raw_execute(
                "SELECT l.trip_id, l.stop_sequence "
                "FROM stop_times l "
                "INNER JOIN stop_times r ON ("
                "  l.trip_id = r.trip_id"
                "  AND l.stop_id = r.stop_id"
                "  AND l.stop_sequence + 1 = r.stop_sequence"
                ")"
            )
        ]

    def solve_duplicates(
        self,
        db: DBConnection,
        duplicates: Iterable[tuple[str, int]],
    ) -> list[StopTime]:
        return [
            self.solve_duplicate(db, trip_id, stop_sequence)
            for trip_id, stop_sequence in duplicates
        ]

    def solve_duplicate(self, db: DBConnection, trip_id: str, stop_sequence: int) -> StopTime:
        l = db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
            StopTime,
            (trip_id, stop_sequence),
        ).one_must("solve_duplicate: non-existing (trip_id, stop_sequence) pair")
        r = db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
            StopTime,
            (trip_id, stop_sequence + 1),
        ).one_must("solve_duplicate: non-existing (trip_id, stop_sequence + 1) pair")
        assert l.stop_id == r.stop_id
        return self.merge_stop_times(l, r)

    def merge_stop_times(self, l: StopTime, r: StopTime) -> StopTime:
        return StopTime(
            trip_id=l.trip_id,
            stop_id=l.stop_id,
            stop_sequence=l.stop_sequence,
            arrival_time=l.arrival_time,
            departure_time=r.departure_time,
            pickup_type=merge_pax_exchange(l.pickup_type, r.pickup_type),
            drop_off_type=merge_pax_exchange(l.drop_off_type, r.drop_off_type),
            stop_headsign=r.stop_headsign or l.stop_headsign,
            shape_dist_traveled=(
                l.shape_dist_traveled
                if l.shape_dist_traveled is not None
                else r.shape_dist_traveled
            ),
            platform=r.platform or l.platform,
            extra_fields_json=r.extra_fields_json or l.extra_fields_json,
        )

    def apply_solutions(self, db: DBConnection, solutions: Sequence[StopTime]) -> None:
        with db.transaction():
            db.update_many(StopTime, solutions)
            db.raw_execute_many(
                "DELETE FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
                ((st.trip_id, st.stop_sequence + 1) for st in solutions),
            )


def merge_pax_exchange(l: PaxExchange, r: PaxExchange) -> PaxExchange:
    def any_eq(to: PaxExchange) -> bool:
        return l == to or r == to

    if any_eq(PaxExchange.SCHEDULED_STOP):
        return PaxExchange.SCHEDULED_STOP
    elif any_eq(PaxExchange.MUST_PHONE):
        return PaxExchange.MUST_PHONE
    elif any_eq(PaxExchange.ON_REQUEST):
        return PaxExchange.ON_REQUEST
    else:
        assert l == PaxExchange.NONE
        assert r == PaxExchange.NONE
        return PaxExchange.NONE
