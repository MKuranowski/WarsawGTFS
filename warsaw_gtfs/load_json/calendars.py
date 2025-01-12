from collections.abc import Iterable, Mapping
from typing import Any

from impuls.model import Calendar, CalendarException, Date
from impuls.tools.strings import find_non_conflicting_id


def parse_calendars(data: Any) -> Iterable[tuple[int, Calendar]]:
    used_ids = set[str]()
    for calendar in data["typy_dni"]:
        yield parse_calendar(calendar, used_ids)


def parse_calendar(data: Any, used_ids: set[str]) -> tuple[int, Calendar]:
    id = find_non_conflicting_id(used_ids, data["nazwa"], separator="_")
    used_ids.add(id)
    return data["id_typu_dnia"], Calendar(id=id, desc=data["nazwa"])


def parse_calendar_exceptions(
    data: Any,
    calendar_id_lookup: Mapping[int, str],
) -> Iterable[CalendarException]:
    for exception in data["kalendarz"]:
        yield parse_calendar_exception(exception, calendar_id_lookup[exception["id_typu_dnia"]])


def parse_calendar_exception(data: Any, calendar_id: str) -> CalendarException:
    return CalendarException(
        calendar_id,
        Date.fromisoformat(data["data"]),
        CalendarException.Type.ADDED,
    )
