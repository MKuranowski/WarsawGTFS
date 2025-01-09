from argparse import Namespace

from impuls import App, LocalResource, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.tasks import AddEntity, ExecuteSQL, RemoveUnusedEntities

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
                        "               WHERE extra_fields_json ->> 'depot' = 1)"
                    ),
                ),
                RemoveUnusedEntities(),
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
                # TODO: make trips.direction_id consistent for trains (eastbound=0)
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
