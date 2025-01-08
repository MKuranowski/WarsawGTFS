from collections.abc import Iterable
from typing import Any

from impuls.db import SQLRow


def parse_shapes(data: Any) -> Iterable[SQLRow]:
    return ((str(i["id_wariantu"]),) for i in data["warianty"])


def parse_shape_points(data: Any) -> Iterable[SQLRow]:
    return (
        (
            str(i["id_wariantu"]),
            i["numer_punktu"],
            i["gps_n"],
            i["gps_e"],
        )
        for i in data["ksztalt_trasy_GPS"]
    )
