from collections.abc import Iterable
from typing import cast

from impuls import DBConnection
from impuls.task import Task, TaskRuntime


class DropHiddenVariants(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        variant_ids_to_drop = self.get_variants_to_drop(r.db)
        with r.db.transaction():
            self.drop_trips(r.db, variant_ids_to_drop)
            self.drop_shapes(r.db, variant_ids_to_drop)
            self.drop_variants(r.db, variant_ids_to_drop)

    def get_variants_to_drop(self, db: DBConnection) -> list[str]:
        variant_ids_to_drop = list[str]()
        result = db.raw_execute(
            "SELECT v.variant_id, v.route_id, v.code, count(*) "
            "FROM variant_stops s "
            "LEFT JOIN variants v ON (s.variant_id = v.variant_id) "
            "WHERE v.code LIKE 'TU-%' "
            "GROUP BY s.variant_id "
            "HAVING count(*) <= 2"
        )

        for record in result:
            variant_id = cast(str, record[0])
            route_id = cast(str, record[1])
            code = cast(str, record[2])
            count = cast(int, record[3])

            variant_ids_to_drop.append(variant_id)
            self.logger.warning(
                "Dropping variant %s (%r) on route %s with %d stops",
                variant_id,
                code,
                route_id,
                count,
            )

        return variant_ids_to_drop

    def drop_trips(self, db: DBConnection, variant_ids: Iterable[str]) -> None:
        db.raw_execute_many(
            "DELETE FROM trips WHERE shape_id = ?",
            ((id,) for id in variant_ids),
        )

    def drop_shapes(self, db: DBConnection, variant_ids: Iterable[str]) -> None:
        db.raw_execute_many(
            "DELETE FROM shapes WHERE shape_id = ?",
            ((id,) for id in variant_ids),
        )

    def drop_variants(self, db: DBConnection, variant_ids: Iterable[str]) -> None:
        db.raw_execute_many(
            "DELETE FROM variants WHERE variant_id = ?",
            ((id,) for id in variant_ids),
        )
