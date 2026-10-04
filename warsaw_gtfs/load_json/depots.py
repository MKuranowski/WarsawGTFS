from typing import Any


def parse_depots(data: Any) -> dict[int, str]:
    return {i["id_zajezdni"]: i["nazwa_zajezdni"] for i in data["zajezdnie_przewoznicy"]}
