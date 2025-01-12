from collections.abc import Iterable
from enum import Enum
from typing import Any

from impuls.model import Route
from impuls.tools.strings import find_non_conflicting_id


class RouteDescription(Enum):
    TRAM = 0
    NORMAL_BUS = 1
    EXPRESS_BUS = 2
    SUBURBAN_BUS = 3
    RURAL_BUS = 4
    NIGHT_BUS = 5
    TRAIN_SKM = 6
    TRAIN_KM = 7
    TRAIN_WKD = 8

    @classmethod
    def from_name(cls, name: str) -> "RouteDescription":
        name = name.upper()
        if "TRAM" in name:
            return cls.TRAM
        elif "KOLEI DOJ" in name:
            return cls.TRAIN_WKD
        elif "KOLEI MAZ" in name:
            return cls.TRAIN_KM
        elif "KOLEI" in name:
            return cls.TRAIN_SKM
        elif "NOC" in name:
            return cls.NIGHT_BUS
        elif "UZUP" in name:
            return cls.RURAL_BUS
        elif "STREF" in name:
            return cls.SUBURBAN_BUS
        elif "EKSPR" in name or "PRZYSP" in name:
            return cls.EXPRESS_BUS
        else:
            return cls.NORMAL_BUS

    def type(self) -> Route.Type:
        match self:
            case self.TRAM:
                return Route.Type.TRAM
            case (
                self.NORMAL_BUS
                | self.EXPRESS_BUS
                | self.SUBURBAN_BUS
                | self.RURAL_BUS
                | self.NIGHT_BUS
            ):
                return Route.Type.BUS
            case self.TRAIN_SKM | self.TRAIN_KM | self.TRAIN_WKD:
                return Route.Type.RAIL
        raise RuntimeError(f"unexpected {self}")

    def color(self, route_name: str) -> tuple[str, str]:
        match self, route_name:
            case self.TRAM, _:
                return "B60000", "FFFFFF"
            case self.NORMAL_BUS, _:
                return "880077", "FFFFFF"
            case self.EXPRESS_BUS, _:
                return "B60000", "FFFFFF"
            case self.SUBURBAN_BUS, _:
                return "006800", "FFFFFF"
            case self.RURAL_BUS, _:
                return "000088", "FFFFFF"
            case self.NIGHT_BUS, _:
                return "000000", "FFFFFF"
            case self.TRAIN_SKM, "S1" | "S10" | "S11":
                return "E84A4B", "FFFFFF"
            case self.TRAIN_SKM, "S2" | "S20":
                return "2E8EC8", "FFFFFF"
            case self.TRAIN_SKM, "S3" | "S30":
                return "FFAC01", "000000"
            case self.TRAIN_SKM, "S4":
                return "2F7B20", "FFFFFF"
            case self.TRAIN_SKM, "S40":
                return "70AD46", "FFFFFF"
            case self.TRAIN_SKM | self.TRAIN_KM | self.TRAIN_WKD, _:
                return "009955", "FFFFFF"
        raise RuntimeError(f"unexpected {self}")


def parse_routes(data: Any) -> Iterable[tuple[int, Route]]:
    descriptions = parse_route_descriptions(data["typy_linii"])
    used_ids = set[str]()
    for route in data["linie"]:
        yield parse_route(route, descriptions[route["id_typu_linii"]], used_ids)


def parse_route(data: Any, desc: RouteDescription, used_ids: set[str]) -> tuple[int, Route]:
    name = data["nazwa"].strip()
    type = desc.type()
    color, text_color = desc.color(name)
    id = find_non_conflicting_id(used_ids, name)
    used_ids.add(id)
    return data["id_linii"], Route(id, "0", name, "", type, color, text_color)


def parse_route_descriptions(data: Any) -> dict[int, RouteDescription]:
    return {i["id_typu_linii"]: RouteDescription.from_name(i["nazwa_typu_linii"]) for i in data}
