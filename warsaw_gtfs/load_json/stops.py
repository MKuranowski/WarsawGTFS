from collections.abc import Iterable
from typing import Any, NamedTuple

from impuls.model import Stop

from ..util import compact_json


class Group(NamedTuple):
    code: str
    full_name: str
    name: str
    town: str


def parse_stops(data: Any) -> Iterable[Stop]:
    groups = parse_groups(data)
    return (parse_stop(i, groups[i["id_przystanku"]]) for i in data["slupki"])


def parse_stop(data: Any, group: Group) -> Stop:
    code_within_group = data["nazwa_slupka"]
    code = f"{group.code}{code_within_group}"
    return Stop(
        str(data["id_slupka"]),
        group.full_name,
        data["gps_n"],
        data["gps_e"],
        code=code,
        extra_fields_json=compact_json(
            {
                "stop_name_stem": group.name,
                "town_name": group.town,
                "depot": data["zajezdnia"],
                "code_within_group": code_within_group,
            }
        ),
    )


def parse_groups(data: Any) -> dict[int, Group]:
    return {i["id_przystanku"]: parse_group(i) for i in data["przystanki"]}


def parse_group(data: Any) -> Group:
    code = data["symbol_przystanku"]
    name = data["nazwa_przystanku"]
    town = data["nazwa_obszaru"]
    if should_add_town_name(code, name, town):
        full_name = f"{town} {name}"
    else:
        full_name = name
    return Group(code, full_name, name, town or "Warszawa")


def should_add_town_name(code: str, name: str, town: str) -> bool:
    # No for stops in Warsaw
    if town == "":
        return False

    # No for railway stops
    if is_railway(code):
        return False

    # No for stops close to railway stations
    name = name.casefold()
    if "pkp" in name or "wkd" in name:
        return False

    # No if name already contains the town
    town = town.casefold()
    if town in name:
        return False

    # No if name and town intersect, e.g. name="Załubice-Szkoła" and town="Stare Załubice"
    if any(part in name for part in town.split()):
        return False

    # Default to yes, to prevent ambiguous names like "Cmentarz"
    return True


def is_railway(code: str) -> bool:
    return code[1:3] in {"90", "91", "92"} or code == "1930"
