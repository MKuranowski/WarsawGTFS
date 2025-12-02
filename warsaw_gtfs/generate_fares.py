from collections.abc import Iterable, Sequence
from itertools import chain, combinations
from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import FareAttribute, FareRule

TICKETS: list[tuple[str, int | None, float, list[str], str]] = [
    (
        "time:20m:1+2",
        20 * 60,
        3.4,
        ["1", "1+2", "2"],
        "non-l",
    ),
    (
        "time:75m:1+2",
        75 * 60,
        4.4,
        ["1", "1+2"],
        "non-l",
    ),
    (
        "time:90m:1",
        90 * 60,
        7.0,
        ["1", "1+2", "2"],
        "non-l",
    ),
    (
        "24h:1",
        24 * 90 * 60,
        15.0,
        ["1", "1+2"],
        "all",
    ),
    (
        "24h:1+2",
        24 * 90 * 60,
        26.0,
        ["1", "1+2", "2"],
        "all",
    ),
    ("single:1", None, 4.4, ["1", "1+2"], "non-l"),
    ("single:1+2", None, 7.0, ["1", "1+2", "2"], "non-l"),
    ("local:L-1", None, 5.0, [], "L-1"),
    ("local:L-2", None, 5.0, [], "L-2"),
    ("local:L-3", None, 5.0, [], "L-3"),
    ("local:L-4", None, 5.0, [], "L-4"),
    ("local:L-5", None, 5.0, [], "L-5"),
    ("local:L-7", None, 3.0, [], "L-7"),
    ("local:L-8", None, 3.0, [], "L-8"),
    ("local:L-9", None, 3.0, [], "L-9"),
    ("local:L10", None, 3.0, [], "L10"),
    ("local:L11", None, 3.0, [], "L11"),
    ("local:L12", None, 5.0, [], "L12"),
    ("local:L13", None, 5.0, [], "L13"),
    ("local:L14", None, 3.6, [], "L14"),
    ("local:L15", None, 3.6, [], "L15"),
    ("local:L16", None, 3.6, [], "L16"),
    ("local:L17", None, 5.0, [], "L17"),
    ("local:L18", None, 5.0, [], "L18"),
    ("local:L19", None, 5.0, [], "L19"),
    ("local:L23", None, 5.0, [], "L23"),
    ("local:L24", None, 5.0, [], "L24"),
    ("local:L25", None, 5.0, [], "L25"),
    ("local:L26", None, 4.0, [], "L26"),
    ("local:L27", None, 4.0, [], "L27"),
    ("local:L30", None, 3.6, [], "L30"),
    ("local:L31", None, 3.0, [], "L31"),
    ("local:L32", None, 5.0, [], "L32"),
    ("local:L33", None, 5.0, [], "L33"),
    ("local:L34", None, 3.0, [], "L34"),
    ("local:L35", None, 4.0, [], "L35"),
    ("local:L36", None, 4.0, [], "L36"),
    ("local:L37", None, 4.0, [], "L37"),
    ("local:L38", None, 4.0, [], "L38"),
    ("local:L39", None, 5.0, [], "L39"),
    ("local:L40", None, 4.0, [], "L40"),
    ("local:L41", None, 3.0, [], "L41"),
    ("local:L44", None, 4.0, [], "L44"),
    ("local:L45", None, 4.0, [], "L45"),
    ("local:L46", None, 4.0, [], "L46"),
    ("local:L49", None, 3.0, [], "L49"),
    ("local:L52", None, 5.0, [], "L52"),
    ("local:L53", None, 5.0, [], "L53"),
    ("local:L54", None, 5.0, [], "L54"),
    ("local:L55", None, 3.6, [], "L55"),
]


class GenerateFares(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        all_routes = {cast(str, r[0]) for r in r.db.raw_execute("SELECT route_id FROM routes")}
        non_l_routes = sorted(i for i in all_routes if not i.startswith("L"))

        for prefix, time_limit_s, price, all_zones, route_selector in TICKETS:
            routes: list[str] | None = None
            if route_selector == "all":
                routes = None
            elif route_selector == "non-l":
                routes = non_l_routes
            elif route_selector not in all_routes:
                self.logger.warning(
                    "Ticket %s refers to non-existing route %s", prefix, route_selector
                )
                continue
            else:
                routes = [route_selector]

            if all_zones:
                for active_zones in all_combinations(all_zones):
                    fare_id = f"{prefix}:{"_".join(active_zones)}"
                    self.save_fare(r.db, fare_id, time_limit_s, price, active_zones, routes)
            else:
                self.save_fare(r.db, prefix, time_limit_s, price, None, routes)

    def save_fare(
        self,
        db: DBConnection,
        fare_id: str,
        time_limit_s: int | None,
        price: float,
        zones: Sequence[str] | None = None,
        routes: Sequence[str] | None = None,
    ) -> None:
        zones = zones or [""]
        routes = routes or [""]

        with db.transaction():
            db.create(
                FareAttribute(
                    id=fare_id,
                    price=price,
                    currency_type="PLN",
                    payment_method=FareAttribute.PaymentMethod.ON_BOARD,
                    transfers=0 if time_limit_s is None else None,
                    agency_id="0",
                    transfer_duration=time_limit_s,
                )
            )
            db.create_many(
                FareRule,
                (
                    FareRule(fare_id=fare_id, route_id=route_id, contains_id=zone_id)
                    for route_id in routes
                    for zone_id in zones
                ),
            )


def load_non_l_routes(db: DBConnection) -> list[str]:
    return [
        route_id
        for r in db.raw_execute("SELECT route_id FROM routes")
        if not (route_id := cast(str, r[0])).startswith("L")
    ]


def all_combinations(zones: Sequence[str]) -> Iterable[tuple[str, ...]]:
    return chain.from_iterable(combinations(zones, i) for i in range(1, len(zones) + 1))
