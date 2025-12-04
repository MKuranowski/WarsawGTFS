from collections.abc import Iterable
from typing import Any, Mapping, cast

from impuls.db import SQLRow


def parse_variants(data: Any, route_id_lookup: Mapping[int, str]) -> Iterable[SQLRow]:
    for variant in data["warianty"]:
        yield parse_variant(variant, route_id_lookup[variant["id_linii"]])


def parse_variant(data: Any, route_id: str) -> SQLRow:
    if data["war_tam"]:
        direction = 0
    elif data["war_powrotny"]:
        direction = 1
    else:
        direction = None

    return (
        str(data["id_wariantu"]),
        route_id,
        (data["nazwa_wariantu"] or "").upper(),
        direction,
        data["war_glowny"] or 0,
        data["war_dojazd"] or 0,
    )


def parse_variant_stops(
    data: Any,
    stop_id_lookup: Mapping[int, str],
    zone_id_lookup: Mapping[int, str],
) -> Iterable[SQLRow]:
    for variant_stop in data["odcinki"]:
        stop_int_id = cast(int, variant_stop["id_slupka"])
        stop_id = stop_id_lookup[stop_int_id]
        zone_int_id = cast(int | None, variant_stop["id_strefy"])
        zone_id = zone_id_lookup[zone_int_id] if zone_int_id is not None else ""

        yield parse_variant_stop(variant_stop, stop_id, zone_id)


def parse_variant_stop(data: Any, stop_id: str, zone_id: str) -> SQLRow:
    return (
        str(data["id_wariantu"]),
        data["numer_trasy"],
        stop_id,
        data["slupek_na_zadanie"] or 0,
        data["slupek_nie_dla_pasazera"] or 0,
        data["tras_wirtualny"] or 0,
        data["stopien_dostepnosci_slupka_dla_niepelnosprawnych"],
        zone_id,
        data["tras_granica_taryf"] or 0,
    )
