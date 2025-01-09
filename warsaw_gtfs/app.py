from argparse import Namespace

from impuls import App, LocalResource, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.tasks import AddEntity, ExecuteSQL

from .load_json import LoadJSON


class WarsawGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
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
                LoadJSON(),
                ExecuteSQL(
                    "DropNonSkmRailRoutes",
                    "DELETE FROM routes WHERE type = 2 AND short_name NOT LIKE 'S%'",
                ),
                # TODO: stop_times.variant_id -> trips.shape_id
                # TODO: variant_stops.accessibility -> stops.wheelchair_accessible
                # TODO: variant_stops.is_request, variant_stops.is_not_available, stops.depot
                #       -> stop_times.pickup_type & drop_off_type
                # TODO: missing variants.direction
                # TODO: variants.direction -> trips.direction_id
                # TODO: variants.is_exceptional -> trips.exceptional
                # TODO: make trips.direction_id consistent for trains (eastbound=0)
                # TODO: drop inaccessible stops
                # TODO: merge duplicate stops
                # TODO: cleanup unused trips
                # TODO: generate trip_headsign
                # TODO: generate route_long_name based on is_main variants
                # TODO: stabilize ids:
                #       routes: short_name
                #       calendars: desc (?)
                #       stops: code
                #       trips: route:calendar:brigade:start_time
                # TODO: save & sort GTFS
            ],
            resources={
                "rozklady.json": LocalResource("ignore_rozklady.json"),
                "slowniki.json": LocalResource("ignore_slowniki.json"),
            },
            options=options,
        )

    # TODO: MultiFile with pulling data from ZTM
    # TODO: Pull metro & add extra "skm-only" export
