from dataclasses import dataclass
from typing import Dict, Generic, Iterator, List, Literal, Optional, Set, Tuple, TypeVar

"""
Utility stuff used only by the Converter object.
"""


# Type helpers

_T = TypeVar("_T")


@dataclass
class FileNamespace(Generic[_T]):
    # This should be a NamedTuple, but somehow typing.NamedTuple can't also be Generic.
    # So, this is a workaround with a dataclass
    routes: _T
    trips: _T
    times: _T
    dates: _T

    def __iter__(self) -> Iterator[_T]:
        """Yields all files from this FileNamespace"""
        # This is an ugly workaroud. In NamedTuple this worked fine,
        # however with dataclasses something like astuple or asdict break,
        # since there's io streams can't be pickled.
        yield self.routes
        yield self.trips
        yield self.times
        yield self.dates


DirStopsType = Dict[Literal["0", "1"], Set[str]]


# Data conversion helpers


def get_route_color_type(id: str, desc: str) -> Tuple[str, str, str]:
    """Get route_type, route_color, route_text_color based on route's id and description."""
    desc = desc.casefold()
    if "kolei" in desc:
        return "2", "009955", "FFFFFF"
    elif "tram" in desc:
        return "0", "B60000", "FFFFFF"
    elif "specjalna" in desc and id in {"W", "M"}:
        return "0", "B60000", "FFFFFF"
    elif "nocna" in desc:
        return "3", "000000", "FFFFFF"
    elif "uzupełniająca" in desc:
        return "3", "000088", "FFFFFF"
    elif "strefowa" in desc:
        return "3", "006800", "FFFFFF"
    elif "ekspresowa" in desc or "przyspieszona" in desc:
        return "3", "B60000", "FFFFFF"
    else:
        return "3", "880077", "FFFFFF"


def get_trip_direction(trip_original_stops: Set[str], direction_stops: DirStopsType) \
        -> Literal["0", "1"]:
    """Guess the trip direction_id based on trip_original_stops, and
    a direction_stops which should be a dictionary with 2 keys: "0" and "1" -
    corresponding values should be sets of stops encountered in given dir
    """
    # Stops for each direction have to be unique
    dir_stops_0 = direction_stops["0"].difference(direction_stops["1"])
    dir_stops_1 = direction_stops["1"].difference(direction_stops["0"])

    # Trip stops in direction 0 and direction 1
    trip_stops_0 = trip_original_stops.intersection(dir_stops_0)
    trip_stops_1 = trip_original_stops.intersection(dir_stops_1)

    # Amount of stops of trip in each direction
    trip_stops_0_len = len(trip_stops_0)
    trip_stops_1_len = len(trip_stops_1)

    # More or equal stops belonging to dir_0 then dir_1 => "0"
    if trip_stops_0_len >= trip_stops_1_len:
        return "0"

    # More stops belonging to dir_1
    elif trip_stops_0_len < trip_stops_1_len:
        return "1"

    # How did we get here
    else:
        raise RuntimeError(f"{trip_stops_0_len} is not bigger, equal or less then "
                           f"{trip_stops_1_len}")


def get_proper_headsign(stop_id: str, stop_name: str):
    """Get trip_headsign based on last stop_id and its stop_name"""
    if stop_id in ["503803", "503804"]:
        return "Zjazd do zajezdni Wola"
    elif stop_id == "103002":
        return "Zjazd do zajezdni Praga"
    elif stop_id == "324010":
        return "Zjazd do zajezdni Mokotów"
    elif stop_id in {"606107", "606108"}:
        return "Zjazd do zajezdni Żoliborz"
    elif stop_id.startswith("4202"):
        return "Lotnisko Chopina"
    else:
        return stop_name


def match_day_type(used_day_types: Set[str], possible_day_types: List[str]) -> Optional[str]:
    """Determines which day_type this route uses on given range of possible day types"""
    for day_type in possible_day_types:
        if day_type in used_day_types:
            return day_type
    return None
