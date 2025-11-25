from typing import NotRequired, TypedDict, cast

from impuls import Task, TaskRuntime
from impuls.tools.geo import earth_distance_m

MAX_DISTANCE_M = 100.0


class CuratedStop(TypedDict):
    lat: float
    lon: float
    distance: NotRequired[float]


class CurateStopPositions(Task):
    def __init__(self, resource: str, max_distance_m: float = MAX_DISTANCE_M) -> None:
        super().__init__()
        self.resource = resource
        self.max_distance_m = max_distance_m

    def execute(self, r: TaskRuntime) -> None:
        to_curate = cast(dict[str, CuratedStop], r.resources[self.resource].json())
        curated = 0

        with r.db.transaction():
            for stop_id, data in to_curate.items():
                current_pos = r.db.raw_execute(
                    "SELECT lat, lon FROM stops WHERE stop_id = ?",
                    (stop_id,),
                ).one()

                if current_pos is None:
                    self.logger.warning("Stop %s does not exist in DB, skipping curation", stop_id)
                    continue

                current_pos = cast(tuple[float, float], current_pos)
                new_pos = data["lat"], data["lon"]
                max_distance = data.get("distance", self.max_distance_m)
                distance = earth_distance_m(*current_pos, *new_pos)

                if distance > max_distance:
                    self.logger.warning(
                        "Stop %s would be moved too much - by %.0f meters (max is %.0f m)",
                        stop_id,
                        distance,
                        max_distance,
                    )
                    continue

                curated += 1
                r.db.raw_execute(
                    "UPDATE stops SET lat = ?, lon = ? WHERE stop_id = ?",
                    (*new_pos, stop_id),
                )

        self.logger.info("Curated %d / %d stop positions", curated, len(to_curate))
