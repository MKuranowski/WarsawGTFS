from typing import Any, NamedTuple


class VehicleKind(NamedTuple):
    name: str
    accessible: bool


def parse_vehicle_kinds(data: Any) -> dict[int, VehicleKind]:
    return {
        i["id_taboru"]: VehicleKind(
            i["nazwa"],
            i["niskopodlogowy"] or i["niskowejsciowy"],
        )
        for i in data["rodzaj_taboru"]
    }
