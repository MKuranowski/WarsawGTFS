from typing import cast

from impuls import DBConnection, Task, TaskRuntime


class GenerateRouteLongNames(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            routes = [cast(str, i[0]) for i in r.db.raw_execute("SELECT route_id FROM routes")]
            names = {route: generate_name(r.db, route) for route in routes}
            r.db.raw_execute_many(
                "UPDATE routes SET long_name = ? WHERE route_id = ?",
                ((name, route) for route, name in names.items()),
            )


def generate_name(db: DBConnection, route: str) -> str:
    headsign_0 = get_most_prominent_headsign(db, route, 0)
    headsign_1 = get_most_prominent_headsign(db, route, 1)
    if headsign_0 and headsign_1:
        return f"{headsign_1} – {headsign_0}"
    elif headsign_0:
        return f"{headsign_0} – {headsign_0}"
    elif headsign_1:
        return f"{headsign_1} – {headsign_1}"
    return ""


def get_most_prominent_headsign(db: DBConnection, route: str, direction: int) -> str | None:
    if v := get_most_prominent_variant(db, route, direction):
        return get_variant_headsign(db, v)
    return None


def get_most_prominent_variant(db: DBConnection, route: str, direction: int) -> str | None:
    """get_most_prominent_variant returns the most common (by the number of trips)
    main variant of a route in a given direction, or if there are no main variants,
    any most common non-exceptional variant.

    If there are no matching variants, returns ``None``.
    """
    result = db.raw_execute(
        (
            "SELECT variant_id FROM variants "
            "WHERE route_id = ? AND direction = ? AND is_exceptional = 0 "
            "ORDER BY is_main * 1000000 + (SELECT COUNT(*) FROM trips WHERE shape_id = variant_id)"
        ),
        (route, direction),
    ).one()
    return cast(str, result[0]) if result else None


def get_variant_headsign(db: DBConnection, variant: str) -> str:
    return cast(
        str,
        db.raw_execute(
            (
                "SELECT stops.name FROM variant_stops "
                "LEFT JOIN stops ON variant_stops.stop_id = stops.stop_id "
                "WHERE variant_id = ? "
                "ORDER BY stop_sequence DESC LIMIT 1"
            ),
            (variant,),
        ).one_must("all variants must have at least one stop")[0],
    )
