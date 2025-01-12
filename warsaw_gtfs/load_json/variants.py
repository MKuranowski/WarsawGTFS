from collections.abc import Iterable
from typing import Any, Mapping

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
        data["war_dojazd"] or data["war_kurs_skrocony"] or 0,
    )


def parse_variant_stops(data: Any, stop_id_lookup: Mapping[int, str]) -> Iterable[SQLRow]:
    for variant_stop in data["odcinki"]:
        yield parse_variant_stop(variant_stop, stop_id_lookup[variant_stop["id_slupka"]])


def parse_variant_stop(data: Any, stop_id: str) -> SQLRow:
    return (
        str(data["id_wariantu"]),
        data["numer_trasy"],
        stop_id,
        data["slupek_na_zadanie"] or 0,
        data["slupek_nie_dla_pasazera"] or 0,
        data["tras_wirtualny"] or 0,
        data["stopien_dostepnosci_slupka_dla_niepelnosprawnych"],
    )
