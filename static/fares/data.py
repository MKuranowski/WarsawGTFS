from typing import Iterable, List, Literal, Optional, TypedDict, Union


"""
Constants describing fares.
"""


# Type Helpers

class _RegularFare(TypedDict):
    id: str
    price: str
    zones: List[str]
    in_L: bool

    duration: Union[Literal[""], int]
    transfers: Literal["", "0"]


class _LFare(TypedDict):
    id: str
    price: str
    zone_constraint: Optional[str]
    routes: Iterable[str]


# Data

REGULAR_FARES: "List[_RegularFare]" = [
    {
        "id": "20min_ZONE1+2",
        "price": "3.40",
        "zones": ["1", "1/2", "2", "2/O"],
        "in_L": False,
        "duration": 20 * 60,
        "transfers": "",
    },
    {
        "id": "75min-Timed_ZONE1",
        "price": "4.40",
        "zones": ["1", "1/2"],
        "in_L": False,
        "duration": 75 * 60,
        "transfers": "",
    },
    {
        "id": "75min-Trip_ZONE1",
        "price": "4.40",
        "zones": ["1", "1/2"],
        "in_L": False,
        "duration": "",
        "transfers": "0",
    },
    {
        "id": "90min-Timed_ZONE1+2",
        "price": "7.00",
        "zones": ["1", "1/2", "2", "2/O"],
        "in_L": False,
        "duration": 90 * 60,
        "transfers": "",
    },
    {
        "id": "90min-Trip_ZONE1+2",
        "price": "7.00",
        "zones": ["1", "1/2", "2", "2/O"],
        "in_L": False,
        "duration": "",
        "transfers": "0",
    },
    {
        "id": "24h_ZONE1",
        "price": "15.00",
        "zones": ["1", "1/2"],
        "in_L": True,
        "duration": 24 * 60 * 60,
        "transfers": "",
    },
    {
        "id": "24h_ZONE1+2",
        "price": "26.00",
        "zones": ["1", "1/2", "2", "2/O"],
        "in_L": True,
        "duration": 24 * 60 * 60,
        "transfers": "",
    },
    {
        "id": "72h_ZONE1",
        "price": "36.00",
        "zones": ["1", "1/2"],
        "in_L": True,
        "duration": 72 * 60 * 60,
        "transfers": "",
    },
    {
        "id": "72h_ZONE1+2",
        "price": "57.00",
        "zones": ["1", "1/2", "2", "2/O"],
        "in_L": True,
        "duration": 72 * 60 * 60,
        "transfers": "",
    },
]


LROUTE_FARES: "List[_LFare]" = [
    {
        "id": "LRoute-2.00",
        "price": "2.00",
        "zone_constraint": None,
        "routes": [
            "L-1", "L-3", "L-4", "L-6", "L-7", "L18", "L26", "L27",
            "L29", "L35", "L36", "L37", "L38",
        ],
    },
    {
        "id": "LRoute-3.00",
        "price": "3.00",
        "zone_constraint": None,
        "routes": [
            "L-8", "L-9", "L10", "L11", "L31", "L33", "L34", "L41", "L49", "L50",
        ],
    },
    {
        "id": "LRoute-3.00-Otwock",
        "price": "3.00",
        "zone_constraint": "2/O",
        "routes": ["L20", "L22"],
    },
    {
        "id": "LRoute-3.60",
        "price": "3.60",
        "zone_constraint": None,
        "routes": [
            "L14", "L15", "L16", "L21", "L28", "L42", "L30",
        ],
    },
    {
        "id": "LRoute-4.00",
        "price": "4.00",
        "zone_constraint": None,
        "routes": [
            "L20", "L22", "L40", "L43", "L44", "L45", "L47", "L48",
        ],
    },
    {
        "id": "LRoute-5.00",
        "price": "5.00",
        "zone_constraint": None,
        "routes": [
            "L-2", "L-5", "L12", "L17", "L19", "L13", "L24", "L25", "L32", "L39",
        ],
    }
]
