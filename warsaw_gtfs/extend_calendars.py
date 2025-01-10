from collections import defaultdict
from typing import Iterable

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError
from impuls.model import Calendar, CalendarException, Date
from impuls.resource import ManagedResource
from impuls.tools import polish_calendar_exceptions
from impuls.tools.temporal import date_range

WEEKDAY_NAMES = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
FALLBACK_WEEKDAYS = [
    [1, 2, 3, 4],
    [2, 3, 0, 4],
    [1, 3, 0, 4],
    [1, 2, 0, 4],
    [3, 2, 1, 0],
    [6],
    [5],
]


class ExtendSchedules(Task):
    def __init__(self, start_date: Date | None = None, duration_days: int = 30) -> None:
        super().__init__()
        self.start_date = start_date or Date.today()
        self.end_date = self.start_date.add_days(duration_days)

        self.holidays = set[Date]()
        self.calendar_map = defaultdict[Date, list[str]](list)
        self.template = list[Date | None]()

    def execute(self, r: TaskRuntime) -> None:
        self.check_all_calendars_use_exceptions(r.db)
        self.load_holidays(r.resources["calendar_exceptions.csv"])
        self.map_days_to_calendars(r.db)
        self.find_template_days_for_extension()
        self.fill_template_days_with_fallback_weekdays()
        with r.db.transaction():
            r.db.create_many(CalendarException, self.generate_extension_exceptions())

    @staticmethod
    def check_all_calendars_use_exceptions(db: DBConnection) -> None:
        calendars = db.retrieve_all(Calendar)
        for calendar in calendars:
            if (
                calendar.start_date != Date.SIGNALS_EXCEPTIONS
                or calendar.end_date != Date.SIGNALS_EXCEPTIONS
                or calendar.compressed_weekdays != 0
            ):
                raise DataError("ExtendSchedules expects all calendars to use exceptions")

    def load_holidays(self, calendar_exceptions_resource: ManagedResource) -> None:
        self.holidays = {
            date
            for date, exception in polish_calendar_exceptions.load_exceptions(
                calendar_exceptions_resource, polish_calendar_exceptions.PolishRegion.MAZOWIECKIE
            ).items()
            if polish_calendar_exceptions.CalendarExceptionType.HOLIDAY in exception.typ
        }

    def map_days_to_calendars(self, db: DBConnection) -> None:
        self.calendar_map.clear()
        for exception in db.retrieve_all(CalendarException):
            if exception.exception_type is CalendarException.Type.ADDED:
                self.calendar_map[exception.date].append(exception.calendar_id)

    def find_template_days_for_extension(self) -> None:
        self.template: list[Date | None] = [None] * 7
        for day in self.calendar_map:
            if day not in self.holidays:
                self.template[day.weekday()] = day

    def fill_template_days_with_fallback_weekdays(self) -> None:
        assert len(self.template) == 7
        for weekday in range(7):
            if self.template[weekday] is None:
                for fallback_weekday in FALLBACK_WEEKDAYS[weekday]:
                    if self.template[fallback_weekday] is not None:
                        self.template[weekday] = self.template[fallback_weekday]
                        self.logger.warning(
                            "Using %s (%s) schedules for extending over %s",
                            self.template[weekday],
                            WEEKDAY_NAMES[fallback_weekday],
                            WEEKDAY_NAMES[weekday],
                        )
                        break
                else:
                    self.logger.error(
                        "No template schedules for extending over %s",
                        WEEKDAY_NAMES[weekday],
                    )
            else:
                self.logger.info(
                    "Using %s schedules for extending over %s",
                    self.template[weekday],
                    WEEKDAY_NAMES[weekday],
                )

    def generate_extension_exceptions(self) -> Iterable[CalendarException]:
        for date in date_range(self.start_date, self.end_date):
            if date not in self.calendar_map:
                weekday = 6 if date in self.holidays else date.weekday()
                template_day = self.template[weekday]
                if template_day:
                    for calendar in self.calendar_map[template_day]:
                        yield CalendarException(
                            calendar_id=calendar,
                            date=date,
                            exception_type=CalendarException.Type.ADDED,
                        )
