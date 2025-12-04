from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import groupby
from operator import itemgetter
from typing import cast

from impuls import DBConnection, Task, TaskRuntime


@dataclass
class StopTimeHash:
    stop_sequence: int
    travel_time: int
    dwell_time: int
    is_not_available: bool = False


class FixZeroTimeSegments(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        self.logger.debug("Finding trips to fix")
        trips = self.get_trips_to_fix(r.db)

        self.logger.debug("Fixing zero time segments")
        self.fix_zero_segments(trips)

        self.logger.debug("Saving updated times")
        self.save_fixed_times(r.db, trips)

    def get_trips_to_fix(self, db: DBConnection) -> list[tuple[str, list[StopTimeHash]]]:
        return [
            (trip_id, stop_times)
            for (trip_id, stop_times) in self.generate_all_trips(db)
            if any(i.travel_time == 0 for i in stop_times[1:])
        ]

    def generate_all_trips(self, db: DBConnection) -> Iterable[tuple[str, list[StopTimeHash]]]:
        q = cast(
            Iterable[tuple[str, int, int, int, int]],
            db.raw_execute(
                "SELECT trip_id, stop_sequence, arrival_time, departure_time, drop_off_type = 1 "
                "FROM stop_times "
                "ORDER BY trip_id ASC"
            ),
        )
        for trip_id, rows in groupby(q, key=itemgetter(0)):
            trip_id = cast(str, trip_id)
            times = list[StopTimeHash]()
            prev_dep = 0
            for i, (_, seq, arr, dep, is_not_available) in enumerate(rows):
                dwell = dep - arr
                travel = arr if i == 0 else arr - prev_dep
                times.append(StopTimeHash(seq, travel, dwell, bool(is_not_available)))
                prev_dep = dep
            yield trip_id, times

    def fix_zero_segments(self, trips: Sequence[tuple[str, list[StopTimeHash]]]) -> None:
        for i, (trip_id, times) in enumerate(trips, start=1):
            self.logger.debug("Fixing times in trip %s (%d/%d)", trip_id, i, len(trips))
            fix_zero_travel_time_with_dwell_time(times[1:])
            fix_zero_travel_from_first_virtual_stop(times)
            for segment in find_zero_segments(times):
                fix_zero_segment(segment)

    def save_fixed_times(
        self,
        db: DBConnection,
        trips: Iterable[tuple[str, list[StopTimeHash]]],
    ) -> None:
        with db.transaction():
            db.raw_execute_many(
                "UPDATE stop_times SET arrival_time = ?, departure_time = ? "
                "WHERE trip_id = ? AND stop_sequence = ? ",
                (
                    (arr, dep, trip_id, seq)
                    for trip_id, times in trips
                    for seq, arr, dep in self.reassemble_times(times)
                ),
            )

    @staticmethod
    def reassemble_times(times: Iterable[StopTimeHash]) -> Iterable[tuple[int, int, int]]:
        prev_dep = 0
        for t in times:
            arr = prev_dep + t.travel_time
            dep = arr + t.dwell_time
            yield t.stop_sequence, arr, dep
            prev_dep = dep


def find_zero_segments(times: Sequence[StopTimeHash]) -> Iterable[Sequence[StopTimeHash]]:
    i = 1
    while i < len(times):
        if times[i].travel_time == 0:
            start = i
            end = i + 1
            while end < len(times) and times[end].travel_time == 0:
                end += 1
            end += 1  # also include the first StopTime with non-zero travel-time
            yield times[start:end]
            i = end
        else:
            i += 1


def fix_zero_segment(times: Sequence[StopTimeHash]) -> None:
    assert all(t.travel_time == 0 for t in times[:-1])
    assert all(t.dwell_time == 0 for t in times[:-1])
    if times[-1].travel_time > 0:
        borrow_travel_time_from_last(times)
    else:
        add_travel_time_at_end(times)


def borrow_travel_time_from_last(times: Sequence[StopTimeHash]) -> None:
    time_available = times[-1].travel_time
    assert time_available > len(times), "not enough time to redistribute :^("

    new_travel_time = time_available // len(times)
    leftover = time_available % len(times)

    for t in times[:-1]:
        t.travel_time = new_travel_time

    times[-1].travel_time = new_travel_time + leftover


def add_travel_time_at_end(times: Sequence[StopTimeHash]) -> None:
    new_travel_time = 60 // (len(times) + 1)
    for t in times:
        t.travel_time = new_travel_time


def fix_zero_travel_time_with_dwell_time(times: Iterable[StopTimeHash]) -> None:
    for st in times:
        if st.travel_time == 0 and st.dwell_time != 0:
            st.travel_time, st.dwell_time = st.dwell_time, st.travel_time


def fix_zero_travel_from_first_virtual_stop(times: Sequence[StopTimeHash]) -> None:
    if times[1].travel_time == 0 and times[0].is_not_available:
        times[0].travel_time -= 15
        times[1].travel_time += 15
