from argparse import ArgumentParser, Namespace
from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

from impuls import App, LocalResource, Pipeline, PipelineOptions, Task
from impuls.model import Agency, Attribution, Date, FeedInfo
from impuls.multi_file import IntermediateFeed, MultiFile
from impuls.resource import Resource
from impuls.tasks import AddEntity, ExecuteSQL, RemoveUnusedEntities, SaveGTFS
from impuls.tools import polish_calendar_exceptions

from .add_metro import AddMetro
from .api import ZTMFileProvider, ZTMResource
from .assign_missing_directions import AssignMissingDirections
from .extend_calendars import ExtendSchedules
from .fix_rail_direction_id import FixRailDirectionID
from .generate_route_long_names import GenerateRouteLongNames
from .gtfs import GTFS_HEADERS
from .load_json import LoadJSON
from .merge_duplicate_stops import MergeDuplicateStops
from .merge_virtual_stops import MergeVirtualStops
from .set_feed_version import SetFeedVersion
from .update_trip_headsigns import UpdateTripHeadsigns

TZ = ZoneInfo("Europe/Warsaw")


def create_intermediate_pipeline(
    feed: IntermediateFeed[Resource],
    save_gtfs: bool = False,
) -> list[Task]:
    tasks: list[Task] = [
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
        AddEntity(
            task_name="AddZTMAttribution",
            entity=Attribution(
                id="0",
                organization_name="Data provided by: Zarząd Transportu Miejskiego w Warszawie",
                is_operator=True,
                is_authority=True,
                is_data_source=True,
                url="https://ztm.waw.pl",
            ),
        ),
        AddEntity(
            task_name="AddMKuranAttribution",
            entity=Attribution(
                id="1",
                organization_name="GTFS provided by: Mikołaj Kuranowski",
                is_authority=True,
                is_data_source=True,
                url="https://mkuran.pl/gtfs/",
            ),
        ),
        AddEntity(
            task_name="AddFeedInfo",
            entity=FeedInfo(
                "Mikołaj Kuranowski",
                "https://mkuran.pl/gtfs/",
                lang="pl",
                version=feed.resource.last_modified.strftime("%Y-%m-%d_%H-%M-%S"),
            ),
        ),
        LoadJSON(feed.resource_name),
        ExecuteSQL(
            "DropNonSkmRailRoutes",
            "DELETE FROM routes WHERE type = 2 AND short_name NOT LIKE 'S%'",
        ),
        ExecuteSQL(
            "SetTripShapeIds",
            (
                "UPDATE trips SET shape_id = ("
                "  SELECT extra_fields_json ->> 'variant_id' "
                "  FROM stop_times"
                "  WHERE stop_times.trip_id = trips.trip_id"
                "  LIMIT 1)"
            ),
        ),
        ExecuteSQL(
            "SetStopAccessibility",
            (
                "UPDATE stops SET wheelchair_boarding = iif(stop_id IN "
                "(SELECT DISTINCT variant_stops.stop_id FROM variant_stops "
                "WHERE accessibility >= 6), 0, 1)"
            ),
        ),
        ExecuteSQL(
            "FlagRequestStopTimes",
            (
                "UPDATE stop_times SET pickup_type = 3, drop_off_type = 3 "
                "WHERE (extra_fields_json ->> 'variant_id', stop_sequence) "
                "IN (SELECT variant_id, stop_sequence FROM variant_stops "
                "    WHERE is_request = 1)"
            ),
        ),
        ExecuteSQL(
            "DeleteUnavailableStopTimes",
            (
                "DELETE FROM stop_times "
                "WHERE (extra_fields_json ->> 'variant_id', stop_sequence) "
                "IN (SELECT variant_id, stop_sequence FROM variant_stops "
                "    WHERE is_not_available = 1)"
                "OR stop_id IN (SELECT stop_id FROM stops"
                "               WHERE extra_fields_json ->> 'depot' = '1')"
            ),
        ),
        RemoveUnusedEntities(),
        AssignMissingDirections(),
        ExecuteSQL(
            "SetTripDirection",
            (
                "UPDATE trips SET direction = (SELECT direction FROM variants "
                "WHERE variants.variant_id = trips.shape_id)"
            ),
        ),
        ExecuteSQL(
            "SetTripExceptional",
            (
                "UPDATE trips SET exceptional = (SELECT is_exceptional FROM variants "
                "WHERE variants.variant_id = trips.shape_id)"
            ),
        ),
        MergeDuplicateStops(),
        ExecuteSQL(
            "FixDoubleSpacesInStopNames",
            r"UPDATE stops SET name = re_sub('\s{2,}', ' ', name) WHERE name LIKE '%  %'",
        ),
        # TODO: Fix stop names (e.g. spaces around dashes, not just double spaces)
        MergeVirtualStops(),
        FixRailDirectionID(),
        UpdateTripHeadsigns(),
        GenerateRouteLongNames(),
        ExecuteSQL(
            "RemoveExtraTripShortName",
            (
                "UPDATE trips SET short_name = '' WHERE route_id NOT IN "
                "(SELECT route_id FROM routes WHERE type = 2)"
            ),
        ),
        # TODO: Fix shapes
    ]

    if save_gtfs:
        tasks.append(SaveGTFS(GTFS_HEADERS, "gtfs.zip", ensure_order=True))

    return tasks


def create_final_pipeline(
    feeds: list[IntermediateFeed[LocalResource]],
    force_feed_version: str = "",
) -> list[Task]:
    tasks = list[Task]()
    tasks.append(ExtendSchedules())
    if force_feed_version:
        tasks.append(SetFeedVersion(force_feed_version))
    tasks.append(AddMetro())
    # TODO: Export skm-only GTFS
    tasks.append(SaveGTFS(GTFS_HEADERS, "gtfs.zip", ensure_order=True))
    return tasks


class WarsawGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--single",
            nargs="?",
            metavar="YYYY-MM-DD",
            default=None,
            const=Date.today(),
            type=Date.from_ymd_str,
            help="convert a single feed version (if the argument is missing - use today)",
        )

    def prepare(
        self,
        args: Namespace,
        options: PipelineOptions,
    ) -> Pipeline | MultiFile[ZTMResource]:
        resources = {
            "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
            "metro_routes.csv": LocalResource("data_curated/metro/routes.csv"),
            "metro_schedules.csv": LocalResource("data_curated/metro/schedules.csv"),
            "metro_services.csv": LocalResource("data_curated/metro/services.csv"),
            "metro_stops.csv": LocalResource("data_curated/metro/stops.csv"),
            "metro_variant_stops.csv": LocalResource("data_curated/metro/variant_stops.csv"),
            "metro_variants.csv": LocalResource("data_curated/metro/variants.csv"),
            "tram_rail_shapes.osm": LocalResource("data_curated/tram_rail_shapes.osm"),
        }

        if args.single:
            feed = ZTMFileProvider(args.single).single()
            return Pipeline(
                tasks=create_intermediate_pipeline(feed, save_gtfs=True),
                resources={
                    **resources,
                    feed.resource_name: feed.resource,
                },
                options=options,
            )
        else:
            feed_version = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
            return MultiFile(
                options=options,
                intermediate_provider=ZTMFileProvider(),
                intermediate_pipeline_tasks_factory=create_intermediate_pipeline,
                final_pipeline_tasks_factory=partial(
                    create_final_pipeline,
                    force_feed_version=feed_version,
                ),
                additional_resources=resources,
            )
