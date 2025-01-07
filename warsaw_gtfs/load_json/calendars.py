from collections.abc import Iterable
from typing import Any

from impuls.model import Calendar, CalendarException, Date


def parse_calendars(data: Any) -> Iterable[Calendar]:
    return map(parse_calendar, data["typy_dni"])


def parse_calendar(data: Any) -> Calendar:
    return Calendar(id=str(data["id_typu_dnia"]), desc=data["nazwa"])


def parse_calendar_exceptions(data: Any) -> Iterable[CalendarException]:
    return map(parse_calendar_exception, data["kalendarz"])


def parse_calendar_exception(data: Any) -> CalendarException:
    return CalendarException(
        str(data["id_typu_dnia"]),
        Date.fromisoformat(data["data"]),
        CalendarException.Type.ADDED,
    )
