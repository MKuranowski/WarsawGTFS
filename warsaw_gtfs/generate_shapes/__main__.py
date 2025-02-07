import logging
from argparse import Namespace

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.tasks import LoadDB

from .task import DEFAULT_CONFIGS, GenerateShapes


class GenerateShapesPipeline(App):
    def before_run(self) -> None:
        logging.getLogger("pyroutelib3.osm").setLevel(logging.ERROR)

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            tasks=[
                LoadDB("warsaw.db"),
                GenerateShapes(DEFAULT_CONFIGS),
            ],
            resources={
                "warsaw.db": LocalResource("ignore_warsaw.db"),
                "tram_rail_shapes.osm": LocalResource("data_curated/tram_rail_shapes.osm"),
                "mazowieckie-latest.osm.pbf": HTTPResource.get(
                    "https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf"
                ),
            },
        )


GenerateShapesPipeline().run()
