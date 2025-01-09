from collections import defaultdict
from collections.abc import Iterable, Set
from typing import cast

from impuls import DBConnection, Task, TaskRuntime


class AssignMissingDirections(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            variants_without_directions = self.get_variants_without_directions(r.db)
            for route, variants in variants_without_directions.items():
                self.assign_missing_directions(r.db, route, variants)

    def get_variants_without_directions(self, db: DBConnection) -> defaultdict[str, list[str]]:
        r = defaultdict[str, list[str]](list)
        s = db.raw_execute("SELECT route_id, variant_id FROM variants WHERE direction IS NULL")
        for row in s:
            route_id = cast(str, row[0])
            variant_id = cast(str, row[1])
            r[route_id].append(variant_id)
        return r

    def assign_missing_directions(
        self,
        db: DBConnection,
        route: str,
        variants: Iterable[str],
    ) -> None:
        d0, d1 = self.get_route_direction_stop_sets(db, route)
        for variant in variants:
            self.assign_missing_direction(db, variant, d0, d1)

    def assign_missing_direction(
        self,
        db: DBConnection,
        variant: str,
        d0: Set[str],
        d1: Set[str],
    ) -> None:
        stops = {
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT stop_id FROM variant_stops WHERE variant_id = ?",
                (variant,),
            )
        }

        d0_overlap = len(d0 & stops)
        d1_overlap = len(d1 & stops)
        direction = int(d1_overlap > d0_overlap)
        db.raw_execute(
            "UPDATE variants SET direction = ? WHERE variant_id = ?",
            (direction, variant),
        )

    def get_route_direction_stop_sets(
        self,
        db: DBConnection,
        route_id: str,
    ) -> tuple[set[str], set[str]]:
        d0 = self.get_route_direction_stop_set(db, route_id, 0)
        d1 = self.get_route_direction_stop_set(db, route_id, 1)
        common = d0 & d1
        return d0 - common, d1 - common

    def get_route_direction_stop_set(
        self,
        db: DBConnection,
        route_id: str,
        direction: int,
    ) -> set[str]:
        return {
            cast(str, i[0])
            for i in db.raw_execute(
                (
                    "SELECT stop_id FROM variant_stops "
                    "LEFT JOIN variants ON variant_stops.variant_id = variants.variant_id "
                    "WHERE route_id = ? AND direction = ?"
                ),
                (route_id, direction),
            )
        }
