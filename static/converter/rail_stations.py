from dataclasses import dataclass, field
from typing import (Dict, FrozenSet, List, Literal, Mapping, NamedTuple,
                    Optional, Tuple)
from xml.sax import parse as sax_parse
from xml.sax.handler import ContentHandler as SAXContentHandler


class RailwayPlatform(NamedTuple):
    name: str
    lat: float
    lon: float
    wheelchair: Literal["0", "1", "2"]
    ztm_codes: Optional[FrozenSet[str]]


@dataclass
class RailwayStation:
    name: str
    lat: float
    lon: float
    ibnr: str
    pkpplk: str
    wheelchair: Literal["0", "1", "2"]

    platforms: List[RailwayPlatform] = field(default_factory=list)


def _parse_tristate(value: Optional[str]) -> Literal["0", "1", "2"]:
    if value is None:
        return "0"
    elif value.casefold() == "yes":
        return "1"
    else:
        return "2"


class RailwayStationLoader(SAXContentHandler):
    def __init__(self) -> None:
        super().__init__()
        self.tags: Dict[str, str] = {}
        self.position: Tuple[float, float] = float("nan"), float("nan")
        self.stations: Dict[str, RailwayStation] = {}
        self.platforms: Dict[str, List[RailwayPlatform]] = {}
        self.in_node: bool = False

    def startElement(self, name: str, attrs: Mapping[str, str]):
        if name == "node":
            self.tags = {"_id": attrs["id"]}
            self.position = (float(attrs["lat"]), float(attrs["lon"]))
            self.in_node = True

        elif name == "tag" and self.in_node:
            if attrs["k"].startswith("_"):
                raise ValueError("Starting a tag with an underscore messes with processing")
            self.tags[attrs["k"]] = attrs["v"]

    def endElement(self, name: str):
        if name == "node":
            self.in_node = False

            if self.tags.get("railway") == "station" and "ref:ztmw" in self.tags:
                self.stations[self.tags["ref:ztmw"]] = RailwayStation(
                    self.tags["name"],
                    self.position[0],
                    self.position[1],
                    self.tags.get("ref:ibnr", ""),
                    self.tags["ref"],
                    _parse_tristate(self.tags.get("wheelchair")),
                    [],
                )

            elif self.tags.get("public_transport") == "platform":
                station = self.tags["ref:station"]
                ztm_codes = frozenset(self.tags["ref:ztmw"].split(";")) \
                    if "ref:ztmw" in self.tags else None

                self.platforms.setdefault(station, []).append(RailwayPlatform(
                    self.tags["name"],
                    self.position[0],
                    self.position[1],
                    _parse_tristate(self.tags.get("wheelchair")),
                    ztm_codes,
                ))

    @classmethod
    def load_all(cls, path: str) -> Dict[str, RailwayStation]:
        handler = cls()

        # Load the file

        sax_parse(path, handler)

        # Post-process platforms
        stations_with_missing_platforms: list[str] = []
        for station in handler.stations.values():

            # See if there are platforms
            platforms = handler.platforms.get(station.pkpplk)
            if platforms is None:
                stations_with_missing_platforms.append(station.name)
                continue

            # Attach platforms
            station.platforms = platforms

        # Check if all ZTM stations have platforms
        if stations_with_missing_platforms:
            raise ValueError("Stations without platforms: "
                             + ", ".join(stations_with_missing_platforms))

        return handler.stations
