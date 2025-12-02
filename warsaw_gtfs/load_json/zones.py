from typing import Any


def parse_zones(data: Any) -> dict[int, str]:
    return {i["id_strefy"]: i["symbol"] for i in data["strefy_taryfowe"]}
