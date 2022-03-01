import datetime
from dataclasses import dataclass
from typing import List, Literal, Optional

"""
Classes used when Parser generates data
"""


__all__ = [
    "ZTMCalendar", "ZTMStopGroup", "ZTMStop", "ZTMStopTime", "ZTMTrip",
    "ZTMRouteVariant", "ZTMVariantStop", "ZTMDeparture", "ZTMRoute"
]


@dataclass
class ZTMCalendar:
    __slots__ = ("date", "services")

    date: datetime.date
    services: List[str]


@dataclass
class ZTMStopGroup:
    __slots__ = ("id", "name", "town", "town_code")

    id: str
    name: str
    town: str
    town_code: str


@dataclass
class ZTMStop:
    __slots__ = ("id", "code", "lat", "lon", "wheelchair")

    id: str
    code: str
    lat: Optional[float]
    lon: Optional[float]
    wheelchair: Literal["0", "1", "2"]


@dataclass
class ZTMStopTime:
    __slots__ = ("stop", "original_stop", "time", "flags", "platform")

    stop: str
    original_stop: str
    time: str
    flags: Literal["", "P", "B"]
    platform: str


@dataclass
class ZTMTrip:
    __slots__ = ("id", "train_number", "stops")

    id: str
    train_number: str
    stops: List[ZTMStopTime]


@dataclass
class ZTMRouteVariant:
    __slots__ = ("id", "direction", "variant_order")

    id: str
    direction: Literal["0", "1"]
    variant_order: str


@dataclass
class ZTMVariantStop:
    __slots__ = ("id", "on_demand", "zone")

    id: str
    on_demand: bool
    zone: Literal["1", "1/2", "2", "2/O"]


@dataclass
class ZTMDeparture:
    __slots__ = ("trip_id", "time", "accessible")

    trip_id: str
    time: str
    accessible: bool


@dataclass
class ZTMRoute:
    __slots__ = ("id", "desc")

    id: str
    desc: str
