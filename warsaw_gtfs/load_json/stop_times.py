from collections.abc import Iterable, Mapping
from typing import Any

from impuls.model import StopTime, TimePoint

from ..util import compact_json


def parse_stop_times(
    data: Any,
    trip_id_lookup: Mapping[int, str],
    stop_id_lookup: Mapping[int, str],
) -> Iterable[StopTime]:
    for stop_time in data["kursy_przejazdy"]:
        yield parse_stop_time(
            stop_time,
            trip_id_lookup[stop_time["id_kursu"]],
            stop_id_lookup[stop_time["id_slupka"]],
        )


def parse_stop_time(data: Any, trip_id: str, stop_id: str) -> StopTime:
    return StopTime(
        trip_id=trip_id,
        stop_id=stop_id,
        stop_sequence=data["numer_slupka"],
        arrival_time=TimePoint.from_str(data["p24"]),
        departure_time=TimePoint.from_str(data["o24"]),
        extra_fields_json=compact_json({"variant_id": str(data["id_wariantu"])}),
    )
