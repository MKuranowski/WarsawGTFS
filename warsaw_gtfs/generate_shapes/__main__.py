import argparse
import logging
from datetime import timedelta

import impuls
import routx
from impuls.model import Route
from impuls.resource import TimeLimitedResource

from ..curate_stop_positions import CurateStopPositions
from ..gtfs import GTFS_HEADERS
from .task import GenerateShapes


class GenerateShapesApp(impuls.App):
    def before_run(self) -> None:
        logging.getLogger("routx.osm").setLevel(logging.ERROR)

    def prepare(
        self,
        args: argparse.Namespace,
        options: impuls.PipelineOptions,
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadDB("ignore_shapes_base.db"),
                CurateStopPositions("stop_positions.json"),
                GenerateShapes(
                    routes=impuls.selector.Routes(type=Route.Type.RAIL),
                    osm_resource="tram_rail_shapes.osm",
                    profile=routx.OsmProfile.RAILWAY,
                    overwrite=True,
                    shape_id_prefix="2:",
                    ratio_override_resource="shapes_override_ratios.json",
                    task_name="GenerateRailShapes",
                ),
                GenerateShapes(
                    routes=impuls.selector.Routes(type=Route.Type.TRAM),
                    osm_resource="tram_rail_shapes.osm",
                    profile=routx.OsmProfile.TRAM,
                    overwrite=True,
                    shape_id_prefix="0:",
                    ratio_override_resource="shapes_override_ratios.json",
                    task_name="GenerateTramShapes",
                ),
                GenerateShapes(
                    routes=impuls.selector.Routes(type=Route.Type.BUS),
                    osm_resource="mazowieckie-latest.osm.pbf",
                    profile=routx.OsmProfile.BUS,
                    bbox=(20.58, 51.92, 21.47, 52.5),
                    overwrite=True,
                    shape_id_prefix="3:",
                    ratio_override_resource="shapes_override_ratios.json",
                    dump_errors=True,
                    task_name="GenerateBusShapes",
                ),
                impuls.tasks.SaveGTFS(GTFS_HEADERS, "ignore_gtfs_shapes.zip", ensure_order=True),
            ],
            resources={
                "ignore_shapes_base.db": impuls.LocalResource("ignore_shapes_base.db"),
                "tram_rail_shapes.osm": impuls.LocalResource("data_curated/tram_rail_shapes.osm"),
                "mazowieckie-latest.osm.pbf": TimeLimitedResource(
                    r=impuls.HTTPResource.get(
                        "https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf",
                    ),
                    minimal_time_between=timedelta(days=7),
                ),
                "shapes_override_ratios.json": impuls.LocalResource(
                    "data_curated/shapes_override_ratios.json"
                ),
                "stop_positions.json": impuls.LocalResource("data_curated/stop_positions.json"),
            },
            options=options,
        )


GenerateShapesApp().run()
