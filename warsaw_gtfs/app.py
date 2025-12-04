import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta
from functools import partial
from zoneinfo import ZoneInfo

import routx
from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions, Task, selector
from impuls.model import Agency, Attribution, Date, FeedInfo, Route
from impuls.multi_file import IntermediateFeed, MultiFile
from impuls.resource import Resource, TimeLimitedResource
from impuls.tasks import AddEntity, ExecuteSQL, RemoveUnusedEntities, SaveGTFS
from impuls.tools import polish_calendar_exceptions

from . import generate_shapes
from .add_metro import AddMetro
from .api import ZTMFileProvider, ZTMResource
from .assign_missing_directions import AssignMissingDirections
from .assign_zone_id import AssignZoneId
from .collapse_duplicate_stop_times import CollapseDuplicateStopTimes
from .curate_stop_names import CurateStopNames
from .curate_stop_positions import CurateStopPositions
from .extend_calendars import ExtendSchedules
from .fix_rail_direction_id import FixRailDirectionID
from .generate_fares import GenerateFares
from .generate_route_long_names import GenerateRouteLongNames
from .gtfs import GTFS_HEADERS
from .load_json import LoadJSON
from .merge_duplicate_stops import MergeDuplicateStops
from .merge_virtual_stops import MergeVirtualStops
from .set_feed_version import SetFeedVersion
from .update_trip_headsigns import UpdateTripHeadsigns

TZ = ZoneInfo("Europe/Warsaw")


def get_generate_shapes_tasks() -> list[Task]:
    return [
        AddEntity(
            task_name="AddOSMAttribution",
            entity=Attribution(
                id="2",
                organization_name=(
                    "Bus shapes based on data from: "
                    "© OpenStreetMap contributors (under ODbL license)"
                ),
                is_authority=True,
                is_data_source=True,
                url="https://www.openstreetmap.org/copyright",
            ),
        ),
        generate_shapes.Task(
            generate_shapes.GraphConfig(
                osm_resource="tram_rail_shapes.osm",
                profile=routx.OsmProfile.RAILWAY,
                curation_resource="shapes.json",
            ),
            generate_shapes.GenerateConfig(
                routes=selector.Routes(type=Route.Type.RAIL),
                overwrite=True,
                shape_id_prefix="2:",
            ),
            generate_shapes.LoggingConfig(
                task_name="GenerateRailShapes",
            ),
        ),
        generate_shapes.Task(
            generate_shapes.GraphConfig(
                osm_resource="tram_rail_shapes.osm",
                profile=routx.OsmProfile.TRAM,
                curation_resource="shapes.json",
            ),
            generate_shapes.GenerateConfig(
                routes=selector.Routes(type=Route.Type.TRAM),
                overwrite=True,
                shape_id_prefix="0:",
            ),
            generate_shapes.LoggingConfig(
                task_name="GenerateTramShapes",
            ),
        ),
        generate_shapes.Task(
            generate_shapes.GraphConfig(
                osm_resource="mazowieckie-latest.osm.pbf",
                profile=routx.OsmProfile.BUS,
                bbox=(20.58, 51.92, 21.47, 52.5),
                curation_resource="shapes.json",
            ),
            generate_shapes.GenerateConfig(
                routes=selector.Routes(type=Route.Type.BUS),
                overwrite=True,
                shape_id_prefix="3:",
            ),
            generate_shapes.LoggingConfig(
                task_name="GenerateBusShapes",
                dump_errors="shape_errors",
            ),
        ),
    ]


def get_generate_shapes_resources() -> dict[str, Resource]:
    return {
        "tram_rail_shapes.osm": LocalResource("data_curated/tram_rail_shapes.osm"),
        "mazowieckie-latest.osm.pbf": TimeLimitedResource(
            r=HTTPResource.get(
                "https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf",
            ),
            minimal_time_between=timedelta(days=3),
        ),
        "shapes.json": LocalResource("data_curated/shapes.json"),
    }


def get_metro_tasks() -> list[Task]:
    return [
        AddMetro(),
        generate_shapes.Task(
            generate_shapes.GraphConfig(
                osm_resource="tram_rail_shapes.osm",
                profile=routx.OsmProfile.SUBWAY,
            ),
            generate_shapes.GenerateConfig(
                routes=selector.Routes(type=Route.Type.METRO),
                overwrite=True,
                shape_id_prefix="1:",
            ),
            generate_shapes.LoggingConfig(
                task_name="GenerateMetroShapes",
            ),
        ),
    ]


def create_intermediate_pipeline(
    feed: IntermediateFeed[Resource],
    save_gtfs: bool = False,
    generate_shapes: bool = False,
) -> list[Task]:
    tasks: list[Task] = [
        # ========================
        # 1. Create static objects
        # ========================
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
        # ========================
        # 2. Load data & drop anything non-WTP
        # ========================
        LoadJSON(feed.resource_name),
        ExecuteSQL(
            "DropNonSkmRailRoutes",
            "DELETE FROM routes WHERE type = 2 AND short_name NOT LIKE 'S%'",
        ),
        ExecuteSQL(
            "DropKmRoutes",
            "DELETE FROM routes WHERE short_name LIKE 'R%'",
        ),
        # ========================
        # 3. Infer extra variant & variant-stop attributes
        # ========================
        ExecuteSQL(
            "FlagRepositionVariants",  # 2-stop TU-* variants
            (
                "UPDATE variants SET is_exceptional = 1, is_not_available = 1 "
                "WHERE variant_id IN ("
                "  SELECT vs.variant_id FROM variant_stops vs"
                "  LEFT JOIN variants v ON (vs.variant_id = v.variant_id)"
                "  WHERE v.code LIKE 'TU-%'"
                "  GROUP BY vs.variant_id"
                "  HAVING COUNT(*) <= 2"
                ")"
            ),
        ),
        ExecuteSQL(
            "FlagDepotVariants",  # 2-stop variants where one stop is a depot
            (
                "UPDATE variants SET is_exceptional = 1, is_not_available = 1 "
                "WHERE variant_id IN ("
                "  SELECT variant_id FROM variants v"
                "  WHERE"
                "  (SELECT COUNT(*) FROM variant_stops vs WHERE v.variant_id = vs.variant_id) <= 2"
                "  AND EXISTS ("
                "    SELECT 1 FROM variant_stops vs LEFT JOIN stops s ON (vs.stop_id = s.stop_id)"
                "    WHERE vs.variant_id = v.variant_id AND s.extra_fields_json ->> 'depot' = '1'"
                "  )"
                ")"
            ),
        ),
        ExecuteSQL(
            "PropagateNotAvailableVariantsToVariantStops",
            (
                "UPDATE variant_stops SET is_not_available = 1 "
                "WHERE variant_id IN (SELECT variant_id FROM variants WHERE is_not_available = 1)"
            ),
        ),
        AssignMissingDirections(),
        # ========================
        # 4. Infer trip attributes based on variant data
        # ========================
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
            "SetTripVariantCode",
            (
                "UPDATE trips SET "
                "extra_fields_json = json_set("
                "  extra_fields_json,"
                "  '$.variant_code',"
                "  (SELECT code FROM variants WHERE variant_id = shape_id)"
                ")"
            ),
        ),
        ExecuteSQL(
            "DropHiddenVariants",  # All TN-* variants
            "DELETE FROM trips WHERE extra_fields_json ->> 'variant_code' LIKE 'TN-%'",
        ),
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
        # ========================
        # 5. Infer stop-time attributes based on variant-stop data
        # ========================
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
            "FlagUnavailableStopTimes",
            (
                "UPDATE stop_times SET pickup_type = 1, drop_off_type = 1 "
                "WHERE (extra_fields_json ->> 'variant_id', stop_sequence) "
                "IN (SELECT variant_id, stop_sequence FROM variant_stops "
                "    WHERE is_not_available = 1)"
                "OR stop_id IN (SELECT stop_id FROM stops"
                "               WHERE extra_fields_json ->> 'depot' = '1')"
            ),
        ),
        # ========================
        # 6. Infer stop attributes based on variant-stop data
        # ========================
        ExecuteSQL(
            "SetStopAccessibility",
            (
                "UPDATE stops SET wheelchair_boarding = iif(stop_id IN "
                "(SELECT DISTINCT variant_stops.stop_id FROM variant_stops "
                "WHERE accessibility >= 6), 0, 1)"
            ),
        ),
        AssignZoneId(),
        # ========================
        # 7. Run garbage collection
        # ========================
        RemoveUnusedEntities(),
        ExecuteSQL(
            "RemoveUnusedVariants",
            "DELETE FROM variants WHERE NOT EXISTS ("
            "  SELECT 1 FROM trips WHERE trips.shape_id = variants.variant_id"
            ")",
        ),
        # ========================
        # 8. Merge & curate stops
        # ========================
        MergeDuplicateStops(),
        MergeVirtualStops(
            explicit_virtual_stops=[
                "305875",  # Sielce 75 → Sielce 05
                "305876",  # Sielce 76 → Sielce 06
            ],
        ),
        CurateStopNames("stops.json"),
        CurateStopPositions("stops.json"),
        CollapseDuplicateStopTimes(),
        # ========================
        # 9. Prettify other attributes
        # ========================
        # TODO: Fix zero-time segments -- also removing duplicate sequences (see Sielce 05)
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
    ]

    if generate_shapes:
        tasks.extend(get_generate_shapes_tasks())

    if save_gtfs:
        tasks.append(GenerateFares())
        tasks.append(SaveGTFS(GTFS_HEADERS, "gtfs.zip", ensure_order=True))

    return tasks


def create_pre_merge_pipeline(
    feed: IntermediateFeed[Resource],
    drop_shapes: bool = False,
) -> list[Task]:
    tasks = list[Task]()
    if drop_shapes:
        tasks.append(ExecuteSQL("DisableShapes", "UPDATE trips SET shape_id = NULL"))
        tasks.append(ExecuteSQL("TruncateShapes", "DELETE FROM shapes"))
    return tasks


def create_final_pipeline(
    feeds: list[IntermediateFeed[LocalResource]],
    force_feed_version: str = "",
    generate_shapes: bool = False,
) -> list[Task]:
    tasks: list[Task] = [
        GenerateFares(),
        ExtendSchedules(),
    ]
    if force_feed_version:
        tasks.append(SetFeedVersion(force_feed_version))
    tasks.extend(get_metro_tasks())
    if generate_shapes:
        tasks.extend(get_generate_shapes_tasks())

    # TODO: Export skm-only GTFS
    tasks.append(SaveGTFS(GTFS_HEADERS, "gtfs.zip", ensure_order=True))
    return tasks


class WarsawGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "-s",
            "--shapes",
            action="store_true",
            help="generate shapes using OSM data",
        )
        parser.add_argument(
            "--single",
            nargs="?",
            metavar="YYYY-MM-DD",
            default=None,
            const=Date.today(),
            type=Date.from_ymd_str,
            help="convert a single feed version (if the argument is missing - use today)",
        )

    def before_run(self) -> None:
        logging.getLogger("routx.osm").disabled = True

    def prepare(
        self,
        args: Namespace,
        options: PipelineOptions,
    ) -> Pipeline | MultiFile[ZTMResource]:
        resources: dict[str, Resource] = {
            "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
            "stops.json": LocalResource("data_curated/stops.json"),
            "metro_routes.csv": LocalResource("data_curated/metro/routes.csv"),
            "metro_schedules.csv": LocalResource("data_curated/metro/schedules.csv"),
            "metro_services.csv": LocalResource("data_curated/metro/services.csv"),
            "metro_stops.csv": LocalResource("data_curated/metro/stops.csv"),
            "metro_variant_stops.csv": LocalResource("data_curated/metro/variant_stops.csv"),
            "metro_variants.csv": LocalResource("data_curated/metro/variants.csv"),
            "tram_rail_shapes.osm": LocalResource("data_curated/tram_rail_shapes.osm"),
        }

        if args.shapes:
            resources.update(**get_generate_shapes_resources())

        if args.single:
            feed = ZTMFileProvider(args.single).single()
            return Pipeline(
                tasks=create_intermediate_pipeline(
                    feed,
                    save_gtfs=True,
                    generate_shapes=args.shapes,
                ),
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
                pre_merge_pipeline_tasks_factory=partial(
                    create_pre_merge_pipeline,
                    drop_shapes=args.shapes,  # no need to merge shapes if we're overwriting them
                ),
                final_pipeline_tasks_factory=partial(
                    create_final_pipeline,
                    force_feed_version=feed_version,
                    generate_shapes=args.shapes,
                ),
                additional_resources=resources,
            )
