from itertools import chain, combinations, product
from os.path import join
from typing import Iterable, Iterator, Sequence, Tuple, TypeVar
import csv

from .data import _RegularFare, _LFare, REGULAR_FARES, LROUTE_FARES
from ..const import HEADERS

"""
Module containg functionality to generate GTFS fare informations.
"""

_T = TypeVar("_T")


def any_len_combinations(x: Sequence[_T]) -> Iterator[Tuple[_T, ...]]:
    """Yields combinations of x of all possible lengths."""
    len_x = len(x)
    yield from chain.from_iterable(combinations(x, i) for i in range(1, len_x + 1))


def write_rules(rule_writer: "csv._writer", fare_id: str, zones: Iterable[str],
                routes: Iterable[str]):
    """Writes info about valid zones and routes of a particular fare to fare_rules.txt"""

    # Each route has to pass through every mentioned zone for fare to be applicable
    for route, zone in product(routes, zones):
        rule_writer.writerow([fare_id, route, zone])


def write_regular_fare(attr_writer: "csv._writer", rule_writer: "csv._writer",
                       fare: _RegularFare, all_routes: Sequence[str]):
    """Saves info about a RegularFare"""
    # GTFS fare applies if a journey passes through ALL `contains_id` zones.
    # ZTM tickets apply to ANY combination of zones mentioned in `fare["zones"]`.
    # Therefore, a separate fare_id has to be created for every combination of zones in given fare
    for zones in any_len_combinations(fare["zones"]):
        fare_id = fare["id"] + "_COMBINATION" + "+".join(zones)

        # Write to fare_attributes.txt
        attr_writer.writerow([
            fare_id,
            fare["price"],
            "PLN",
            "0",
            fare["transfers"],
            "0",
            fare["duration"],
        ])

        # Filter routes
        if not fare["in_L"]:
            routes = filter(lambda i: not i.startswith("L"), all_routes)
        else:
            # If fare applies to all routes, no route_id should be mentioned in fare_rules.txt
            routes = [""]

        # Write to fare_rules.txt
        write_rules(rule_writer, fare_id, zones, routes)


def write_Lroute_fare(attr_writer: "csv._writer", rule_writer: "csv._writer", fare: _LFare,
                      all_routes: Sequence[str]):
    """Saves info about a LFare"""
    # Filter routes
    routes = [route for route in all_routes if route in fare["routes"]]

    # No active routes applicable for this fare - don't write this fare
    if not routes:
        return

    # Write to fare_attributes.txt
    attr_writer.writerow([
        fare["id"],
        fare["price"],
        "PLN",
        "0",
        "0",
        "0",
        "",
    ])

    # Write to fare_rules.txt
    zones = [fare["zone_constraint"]] if fare["zone_constraint"] is not None else [""]
    write_rules(rule_writer, fare["id"], zones, routes)  # type: ignore


def add_fare_info(target_dir: str, all_routes: Sequence[str]):
    """
    Adds fare information to GTFS stored at {target_dir}.
    A list of all saved routes is required.
    """
    # Determine paths of fare files
    attr_path = join(target_dir, "fare_attributes.txt")
    rule_path = join(target_dir, "fare_rules.txt")

    # Open those files for writing
    with open(attr_path, "w", encoding="utf-8", newline="") as attr_buff, \
            open(rule_path, "w", encoding="utf-8", newline="") as rule_buff:
        # Create CSV writer for fare_attributes
        attr_writer = csv.writer(attr_buff)
        attr_writer.writerow(HEADERS["fare_attributes.txt"])

        # Create CSV writer for fare_rules
        rule_writer = csv.writer(rule_buff)
        rule_writer.writerow(HEADERS["fare_rules.txt"])

        # Save info about regular fares
        for fare in REGULAR_FARES:
            write_regular_fare(attr_writer, rule_writer, fare, all_routes)

        # Save info about L-route fares
        for lfare in LROUTE_FARES:
            write_Lroute_fare(attr_writer, rule_writer, lfare, all_routes)
