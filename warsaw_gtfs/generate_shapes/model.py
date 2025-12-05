from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from math import inf, nan
from typing import NamedTuple, Self, TypeAlias

import routx

BBox: TypeAlias = tuple[float, float, float, float]
LatLon: TypeAlias = tuple[float, float]

StopIdSequence: TypeAlias = tuple[str, int]
StopPair: TypeAlias = tuple[str, str]

TripStops: TypeAlias = tuple[StopIdSequence, ...]
TripIds: TypeAlias = Sequence[str]
TripsByStops: TypeAlias = Mapping[TripStops, TripIds]

ForceVia: TypeAlias = Mapping[StopPair, LatLon]
RatioOverrides: TypeAlias = Mapping[StopPair, float]


class LatLonDist(NamedTuple):
    lat: float
    lon: float
    distance: float = nan

    def with_distance_offset(self, offset: float) -> Self:
        new_dist = round(self.distance + offset, 6)
        return self._replace(distance=new_dist)


class MatchedStop(NamedTuple):
    stop_id: str
    node_id: int
    stop_sequence: int | None = None


@dataclass
class ForceViaPoint:
    lat: float
    lon: float
    node_id: int = 0

    def get_node_id(self, kd_tree: routx.KDTree) -> int:
        if self.node_id == 0:
            self.node_id = kd_tree.find_nearest_node(self.lat, self.lon).id
        return self.node_id


ShapeRequest: TypeAlias = Iterable[StopIdSequence]


@dataclass
class ShapeResponse:
    points: list[LatLonDist] = field(default_factory=list[LatLonDist])
    distances: dict[int, float] = field(default_factory=dict[int, float])


@dataclass
class LegRequest:
    from_: MatchedStop
    to: MatchedStop
    max_distance_ratio: float = inf


@dataclass
class LegResponse:
    from_: MatchedStop
    to: MatchedStop
    points: list[LatLonDist]

    @classmethod
    def prepare(cls, points: list[LatLonDist], request: LegRequest) -> Self:
        return cls(
            request.from_,
            request.to,
            [LatLonDist(round(lat, 6), round(lon, 6), round(dist, 6)) for lat, lon, dist in points],
        )
