from collections.abc import Iterable
from typing import Any, NamedTuple

from impuls.model import Trip

from ..util import compact_json


class Schedule(NamedTuple):
    route_id: str
    calendar_id: str


def parse_trips(data: Any) -> Iterable[Trip]:
    schedules = parse_schedules(data)
    return (parse_trip(i, schedules[i["id_rozkladu"]]) for i in data["rozklady_jazdy"])


def parse_trip(data: Any, schedule: Schedule) -> Trip:
    id = str(data["id_kursu"])
    route_id, calendar_id = schedule
    return Trip(
        id,
        route_id,
        calendar_id,
        short_name=data.get("numer_kursu") or "",
        extra_fields_json=compact_json({"brigade": data.get("brygada_kursu") or ""}),
    )


def parse_schedules(data: Any) -> dict[int, Schedule]:
    return {
        i["id_rozkladu"]: parse_schedule(i)
        for i in data["tabliczki"]
        if i["id_rozkladu"] is not None
    }


def parse_schedule(data: Any) -> Schedule:
    return Schedule(
        route_id=str(data["id_linii"]),
        calendar_id=str(data["id_typu_dnia"]),
    )
