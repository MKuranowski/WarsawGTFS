from collections.abc import Iterable
from typing import Any

from impuls.model import StopTime, TimePoint


def parse_stop_times(data: Any) -> Iterable[StopTime]:
    return map(parse_stop_time, data["kursy_przejazdy"])


def parse_stop_time(data: Any) -> StopTime:
    return StopTime(
        trip_id=str(data["id_kursu"]),
        stop_id=str(data["id_slupka"]),
        stop_sequence=data["numer_slupka"],
        arrival_time=TimePoint.from_str(data["p24"]),
        departure_time=TimePoint.from_str(data["o24"]),
    )
