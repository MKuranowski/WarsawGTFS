from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from impuls.model import Trip

from ..util import compact_json
from .vehicles import VehicleKind


class Schedule(NamedTuple):
    route_id: str
    calendar_id: str


class Job(NamedTuple):
    name: str
    vehicle_kind: int


def parse_trips(data: Any, vehicle_kinds: Mapping[int, VehicleKind] = {}) -> Iterable[Trip]:
    schedules = parse_schedules(data)
    jobs = parse_jobs(data)
    return (
        parse_trip(i, schedules[i["id_rozkladu"]], jobs[i["id_zadania"]], vehicle_kinds)
        for i in data["rozklady_jazdy"]
    )


def parse_trip(
    data: Any,
    schedule: Schedule,
    job: Job,
    vehicle_kinds: Mapping[int, VehicleKind] = {},
) -> Trip:
    id = str(data["id_kursu"])
    route_id, calendar_id = schedule
    if vehicle_kind := vehicle_kinds.get(job.vehicle_kind):
        fleet_type, accessible = vehicle_kind
    else:
        fleet_type, accessible = "", None
    return Trip(
        id,
        route_id,
        calendar_id,
        short_name=data.get("numer_kursu") or "",
        wheelchair_accessible=accessible,
        extra_fields_json=compact_json(
            {
                "hidden_block_id": str(data["id_zadania"]),
                "brigade": data["brygada_kursu"] or "",
                "fleet_type": fleet_type,
            }
        ),
    )


def parse_schedules(data: Any) -> dict[int, Schedule]:
    return {
        i["id_rozkladu"]: Schedule(
            route_id=str(i["id_linii"]),
            calendar_id=str(i["id_typu_dnia"]),
        )
        for i in data["tabliczki"]
        if i["id_rozkladu"] is not None
    }


def parse_jobs(data: Any) -> dict[int, Job]:
    return {
        i["id_zadania"]: Job(
            name=i["nazwa_zadania"],
            vehicle_kind=i["id_taboru"],
        )
        for i in data["zadania"]
    }
