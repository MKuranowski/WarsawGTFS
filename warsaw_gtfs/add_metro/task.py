from collections.abc import Iterable
from typing import cast

from impuls import Task, TaskRuntime
from impuls.model import CalendarException, Date, Frequency, Route, Stop, StopTime, Trip
from impuls.tools import polish_calendar_exceptions

from . import model
from .shape_generator import ShapeGenerator


class AddMetro(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            self.add_routes(r)
            self.add_stops(r)
            self.add_calendars(r)

            variants = self.parse_variants(r)
            self.add_shapes(r, variants.values())
            self.add_trips(r, variants.values())
            self.add_stop_times(r, variants.values())
            self.add_frequencies(r, variants.values())

    def add_routes(self, r: TaskRuntime) -> None:
        r.db.create_many(Route, map(model.parse_route, r.resources["metro_routes.csv"].csv()))

    def add_stops(self, r: TaskRuntime) -> None:
        r.db.create_many(Stop, map(model.parse_stop, r.resources["metro_stops.csv"].csv()))

    def add_calendars(self, r: TaskRuntime) -> None:
        # Get the start and end dates
        start, end = map(
            Date.from_ymd_str,
            cast(
                tuple[str, str],
                # fmt: off
                r.db
                    .raw_execute("SELECT MIN(date), MAX(date) FROM calendar_exceptions")
                    .one_must("no dates in calendar_exceptions"),
                # fmt: on
            ),
        )

        # Get the holidays
        holidays = {
            day
            for day, desc in polish_calendar_exceptions.load_exceptions(
                r.resources["calendar_exceptions.csv"],
                polish_calendar_exceptions.PolishRegion.MAZOWIECKIE,
            ).items()
            if polish_calendar_exceptions.CalendarExceptionType.HOLIDAY in desc.typ
        }

        # Insert into DB
        for row in r.resources["metro_services.csv"].csv():
            service = model.Service.parse(row)
            r.db.create(service.as_calendar())
            r.db.create_many(
                CalendarException,
                service.as_calendar_exceptions(start, end, holidays),
            )

    def parse_variants(self, r: TaskRuntime) -> dict[str, model.Variant]:
        variants = dict[str, model.Variant]()

        for row in r.resources["metro_variants.csv"].csv():
            i = model.Variant.parse(row)
            variants[i.variant_id] = i

        for row in r.resources["metro_variant_stops.csv"].csv():
            i = model.VariantStop.parse(row)
            variants[i.variant_id].stops.append(i)

        for row in r.resources["metro_schedules.csv"].csv():
            i = model.Schedule.parse(row)
            variants[i.variant_id].schedules.append(i)

        return variants

    def add_shapes(self, r: TaskRuntime, variants: Iterable[model.Variant]) -> None:
        with r.resources["tram_rail_shapes.osm"].open_binary() as f:
            generator = ShapeGenerator(f)

        for variant in variants:
            r.db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (variant.variant_id,))
            r.db.raw_execute_many(
                "INSERT INTO shape_points (shape_id,sequence,lat,lon) VALUES (?,?,?,?)",
                (
                    (variant.variant_id, i, lat, lon)
                    for i, (lat, lon) in enumerate(
                        generator.generate_shape(stop.stop_id for stop in variant.stops)
                    )
                ),
            )

    def add_trips(self, r: TaskRuntime, variants: Iterable[model.Variant]) -> None:
        r.db.create_many(Trip, (i for v in variants for i in v.as_trips(v.variant_id)))

    def add_stop_times(self, r: TaskRuntime, variants: Iterable[model.Variant]) -> None:
        r.db.create_many(StopTime, (i for v in variants for i in v.as_stop_times()))

    def add_frequencies(self, r: TaskRuntime, variants: Iterable[model.Variant]) -> None:
        r.db.create_many(Frequency, (i for v in variants for i in v.as_frequencies()))
