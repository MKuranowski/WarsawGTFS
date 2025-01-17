from argparse import Namespace
from datetime import timedelta

from impuls import App, LocalResource, Pipeline, PipelineOptions
from impuls.model import Agency, Calendar, CalendarException, Date
from impuls.tasks import AddEntity, ExecuteSQL
from impuls.tools import polish_calendar_exceptions

from .task import AddMetro


class WarsawMetroGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        start_date = Date.today()
        end_date = start_date + timedelta(days=30)
        return Pipeline(
            tasks=[
                AddEntity(
                    task_name="AddAgency",
                    entity=Agency(
                        id="0",
                        name="Warszawski Transport Publiczny",
                        url="https://wtp.waw.pl",
                        timezone="Europe/Warsaw",
                        lang="pl",
                        phone="+48 22 19 115",
                    ),
                ),
                AddEntity(task_name="AddFakeCalendar", entity=Calendar("fake")),
                AddEntity(
                    task_name="AddFakeCalendarStart",
                    entity=CalendarException("fake", start_date, CalendarException.Type.ADDED),
                ),
                AddEntity(
                    task_name="AddFakeCalendarEnd",
                    entity=CalendarException("fake", end_date, CalendarException.Type.ADDED),
                ),
                AddMetro(),
                ExecuteSQL(
                    task_name="RemoveFakeCalendar",
                    statement="DELETE FROM calendars WHERE calendar_id = 'fake'",
                ),
            ],
            resources={
                "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
                "metro_routes.csv": LocalResource("data_curated/metro/routes.csv"),
                "metro_schedules.csv": LocalResource("data_curated/metro/schedules.csv"),
                "metro_services.csv": LocalResource("data_curated/metro/services.csv"),
                "metro_stops.csv": LocalResource("data_curated/metro/stops.csv"),
                "metro_variant_stops.csv": LocalResource("data_curated/metro/variant_stops.csv"),
                "metro_variants.csv": LocalResource("data_curated/metro/variants.csv"),
                "tram_rail_shapes.osm": LocalResource("data_curated/tram_rail_shapes.osm"),
            },
            options=options,
        )


WarsawMetroGTFS().run()
