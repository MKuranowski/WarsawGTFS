from itertools import repeat
from typing import cast

from impuls.db import DBConnection
from impuls.errors import DataError, MultipleDataErrors
from impuls.task import Task, TaskRuntime

# Pairs of stops that determine the direction_id of a train.
# If train calls at pair[0] before pair[1] - it shall have direction_id = 0;
# else if train calls at pair[1] before pair[0] - it shall have direction_id = 1.
RAIL_DIRECTION_STOPS = [
    ("4900", "7900"),  # W-wa Zachodnia      → W-wa Centralna
    ("5902", "7903"),  # W-wa Zachodnia p. 9 → W-wa Gdańska
    ("4902", "4900"),  # W-wa Włochy         → W-wa Zachodnia
    ("3901", "4917"),  # W-wa Służewiec      → W-wa Rakowiec
    ("1902", "1904"),  # W-wa Praga          → W-wa Żerań
    ("2900", "2910"),  # W-wa Wschodnia      → W-wa Rembertów
    ("2921", "2903"),  # W-wa Grochów        → W-wa Gocławek
    ("1907", "1910"),  # Legionowo           → Wieliszew
]


class FixRailDirectionID(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE trips SET direction = ? WHERE trip_id = ?",
                MultipleDataErrors.catch_all(
                    "direction_id assignment",
                    map(assign_direction_id, repeat(r.db), get_all_train_trips(r.db)),
                ),
            )


def assign_direction_id(db: DBConnection, trip_id: str) -> tuple[int, str]:
    sequence_by_stop_id = {
        cast(str, i[1])[:4]: cast(int, i[0])
        for i in db.raw_execute(
            "SELECT stop_sequence, stop_id FROM stop_times WHERE trip_id = ?",
            (trip_id,),
        )
    }

    for reference_a, reference_b in RAIL_DIRECTION_STOPS:
        idx_a = sequence_by_stop_id.get(reference_a)
        idx_b = sequence_by_stop_id.get(reference_b)
        if idx_a is not None and idx_b is not None:
            return (0 if idx_a < idx_b else 1), trip_id

    raise DataError(f"failed to assign direction_id to trip {trip_id}")


def get_all_train_trips(db: DBConnection) -> list[str]:
    return [
        cast(str, i[0])
        for i in db.raw_execute(
            "SELECT trip_id "
            "FROM trips "
            "LEFT JOIN routes ON (trips.route_id = routes.route_id) "
            "WHERE routes.type = 2"
        )
    ]
