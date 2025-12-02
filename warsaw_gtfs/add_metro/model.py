import re
from collections.abc import Container, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cached_property
from typing import Self

from impuls.model import (
    Calendar,
    CalendarException,
    Date,
    Frequency,
    Route,
    Stop,
    StopTime,
    TimePoint,
    Trip,
)
from impuls.tools.temporal import date_range

WEEKDAY_TO_NAME = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


@dataclass
class Service:
    calendar_id: str
    monday: bool
    tuesday: bool
    wednesday: bool
    thursday: bool
    friday: bool
    saturday: bool
    sunday: bool
    added: set[Date] = field(default_factory=set[Date])
    removed: set[Date] = field(default_factory=set[Date])

    def as_calendar(self) -> Calendar:
        return Calendar(id=self.calendar_id)

    def as_calendar_exceptions(
        self,
        start: Date,
        end: Date,
        holidays: Container[Date],
    ) -> Iterable[CalendarException]:
        for d in date_range(start, end):
            if d not in self.removed and (
                d in self.added
                or getattr(self, WEEKDAY_TO_NAME[self.operational_weekday(d, holidays)])
            ):
                yield CalendarException(self.calendar_id, d, CalendarException.Type.ADDED)

    @staticmethod
    def operational_weekday(d: Date, holidays: Container[Date]) -> int:
        return 6 if d in holidays else d.weekday()

    @classmethod
    def parse(cls, row: Mapping[str, str]) -> Self:
        added = set[Date]()
        removed = set[Date]()

        for exception in re.finditer(r"(-|\+)(\d{8})", row["exceptions"]):
            d = Date.from_ymd_str(exception.group(2))
            if exception.group(1) == "+":
                added.add(d)
            else:
                removed.add(d)

        return cls(
            calendar_id=row["calendar_id"],
            monday=row["monday"] == "1",
            tuesday=row["tuesday"] == "1",
            wednesday=row["wednesday"] == "1",
            thursday=row["thursday"] == "1",
            friday=row["friday"] == "1",
            saturday=row["saturday"] == "1",
            sunday=row["sunday"] == "1",
            added=added,
            removed=removed,
        )


@dataclass
class Schedule:
    route_id: str
    variant_code: str
    calendar_id: str
    start_time: TimePoint
    end_time: TimePoint
    headway: int
    exact: bool

    @cached_property
    def variant_id(self) -> str:
        return f"{self.route_id}:{self.variant_code}"

    @classmethod
    def parse(cls, row: Mapping[str, str]) -> Self:
        return cls(
            route_id=row["route_id"],
            variant_code=row["variant_code"],
            calendar_id=row["calendar_id"],
            start_time=TimePoint.from_str(row["start_time"]),
            end_time=TimePoint.from_str(row["end_time"]),
            headway=int(row["headway"]),
            exact=row["exact"] == "1",
        )


@dataclass
class VariantStop:
    route_id: str
    variant_code: str
    sequence: int
    stop_id: str
    departure_time: TimePoint

    @cached_property
    def variant_id(self) -> str:
        return f"{self.route_id}:{self.variant_code}"

    @classmethod
    def parse(cls, row: Mapping[str, str]) -> Self:
        return cls(
            route_id=row["route_id"],
            variant_code=row["variant_code"],
            sequence=int(row["stop_sequence"]),
            stop_id=row["stop_id"],
            departure_time=TimePoint.from_str(row["departure_time"]),
        )


@dataclass
class Variant:
    route_id: str
    variant_code: str
    direction: Trip.Direction
    headsign: str
    stops: list[VariantStop] = field(default_factory=list[VariantStop])
    schedules: list[Schedule] = field(default_factory=list[Schedule])

    @property
    def variant_id(self) -> str:
        return f"{self.route_id}:{self.variant_code}"

    @cached_property
    def active_calendars(self) -> Sequence[str]:
        return sorted({i.calendar_id for i in self.schedules})

    def as_trips(self, shape_id: str = "") -> Iterable[Trip]:
        for calendar_id in self.active_calendars:
            yield Trip(
                id=f"{self.route_id}:{calendar_id}:{self.variant_code}",
                route_id=self.route_id,
                calendar_id=calendar_id,
                headsign=self.headsign,
                direction=self.direction,
                shape_id=shape_id,
                wheelchair_accessible=True,
                bikes_allowed=True,
            )

    def as_stop_times(self) -> Iterable[StopTime]:
        for calendar_id in self.active_calendars:
            for stop in self.stops:
                yield StopTime(
                    trip_id=f"{self.route_id}:{calendar_id}:{self.variant_code}",
                    stop_id=stop.stop_id,
                    stop_sequence=stop.sequence,
                    arrival_time=stop.departure_time,
                    departure_time=stop.departure_time,
                )

    def as_frequencies(self) -> Iterable[Frequency]:
        for schedule in self.schedules:
            yield Frequency(
                trip_id=f"{self.route_id}:{schedule.calendar_id}:{self.variant_code}",
                start_time=schedule.start_time,
                end_time=schedule.end_time,
                headway=schedule.headway,
                exact_times=schedule.exact,
            )

    @classmethod
    def parse(cls, row: Mapping[str, str]) -> Self:
        return cls(
            route_id=row["route_id"],
            variant_code=row["variant_code"],
            direction=parse_direction(row["direction"]),
            headsign=row["headsign"],
        )


def parse_route(row: Mapping[str, str], agency_id: str = "0") -> Route:
    return Route(
        id=row["route_id"],
        agency_id=agency_id,
        short_name=row["route_id"],
        long_name=row["route_long_name"],
        type=Route.Type.METRO,
        color=row["route_color"],
        text_color=row["route_text_color"],
    )


def parse_stop(row: Mapping[str, str]) -> Stop:
    return Stop(
        id=row["stop_id"],
        code=row.get("stop_code", ""),
        name=row["stop_name"],
        platform_code=row.get("platform_code", ""),
        lat=float(row["stop_lat"]),
        lon=float(row["stop_lon"]),
        zone_id=row.get("zone_id", ""),
        location_type=parse_location_type(row["location_type"]),
        parent_station=row["parent_station"],
        wheelchair_boarding=parse_wheelchair_boarding(row["wheelchair_boarding"]),
    )


def parse_location_type(value: str) -> Stop.LocationType:
    match value:
        case "" | "0":
            return Stop.LocationType.STOP
        case "1":
            return Stop.LocationType.STATION
        case "2":
            return Stop.LocationType.EXIT
        case _:
            raise ValueError(f"invalid location_type: {value!r}")


def parse_wheelchair_boarding(value: str) -> bool | None:
    match value:
        case "" | "0":
            return None
        case "1":
            return True
        case "2":
            return False
        case _:
            raise ValueError(f"invalid wheelchair_boarding: {value!r}")


def parse_direction(value: str) -> Trip.Direction:
    match value:
        case "0":
            return Trip.Direction.OUTBOUND
        case "1":
            return Trip.Direction.INBOUND
        case _:
            raise ValueError(f"invalid direction: {value!r}")
