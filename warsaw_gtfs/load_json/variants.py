from collections.abc import Iterable
from typing import Any

from impuls.db import SQLRow


def parse_variants(data: Any) -> Iterable[SQLRow]:
    return map(parse_variant, data["warianty"])


def parse_variant(data: Any) -> SQLRow:
    if data["war_tam"]:
        direction = 0
    elif data["war_powrotny"]:
        direction = 1
    else:
        direction = None

    return (
        str(data["id_wariantu"]),
        str(data["id_linii"]),
        (data["nazwa_wariantu"] or "").upper(),
        direction,
        data["war_glowny"] or 0,
        data["war_dojazd"] or data["war_kurs_skrocony"] or 0,
    )


def parse_variant_stops(data: Any) -> Iterable[SQLRow]:
    return map(parse_variant_stop, data["odcinki"])


def parse_variant_stop(data: Any) -> SQLRow:
    return (
        str(data["id_wariantu"]),
        data["numer_trasy"],
        str(data["id_slupka"]),
        data["slupek_na_zadanie"] or 0,
        data["slupek_nie_dla_pasazera"] or 0,
        data["tras_wirtualny"] or 0,
        data["stopien_dostepnosci_slupka_dla_niepelnosprawnych"],
    )
