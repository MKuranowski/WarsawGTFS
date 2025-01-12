from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from impuls.model import Trip
from impuls.tools.strings import find_non_conflicting_id

from ..util import compact_json
from .vehicles import VehicleKind


class Schedule(NamedTuple):
    route_id: str
    calendar_id: str


class Job(NamedTuple):
    name: str
    vehicle_kind: int


def parse_trips(
    data: Any,
    route_id_lookup: Mapping[int, str],
    calendar_id_lookup: Mapping[int, str],
    vehicle_kinds: Mapping[int, VehicleKind] = {},
) -> Iterable[tuple[int, Trip]]:
    schedules = parse_schedules(data, route_id_lookup, calendar_id_lookup)
    jobs = parse_jobs(data)
    used_ids = set[str]()
    for trip in data["rozklady_jazdy"]:
        yield parse_trip(
            trip,
            schedules[trip["id_rozkladu"]],
            jobs[trip["id_zadania"]],
            used_ids,
            vehicle_kinds,
        )


def parse_trip(
    data: Any,
    schedule: Schedule,
    job: Job,
    used_ids: set[str],
    vehicle_kinds: Mapping[int, VehicleKind] = {},
) -> tuple[int, Trip]:
    route_id, calendar_id = schedule
    brigade = data["brygada_kursu"] or ""
    departure_time = data["o24"][0:2] + data["o24"][3:5]  # extract HHMM from HH:MM:SS

    route_name = route_id.partition(":")[0]
    calendar_name = calendar_id.partition(":")[0]
    id = find_non_conflicting_id(
        used_ids,
        f"{route_name}:{calendar_name}:{brigade}:{departure_time}",
    )
    used_ids.add(id)

    if vehicle_kind := vehicle_kinds.get(job.vehicle_kind):
        fleet_type, accessible = vehicle_kind
    else:
        fleet_type, accessible = "", None
    return data["id_kursu"], Trip(
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


def parse_schedules(
    data: Any,
    route_id_lookup: Mapping[int, str],
    calendar_id_lookup: Mapping[int, str],
) -> dict[int, Schedule]:
    return {
        i["id_rozkladu"]: Schedule(
            route_id=route_id_lookup[i["id_linii"]],
            calendar_id=calendar_id_lookup[i["id_typu_dnia"]],
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
