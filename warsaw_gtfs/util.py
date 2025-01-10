import json
from typing import Any


def compact_json(data: Any) -> str:
    return json.dumps(data, indent=None, separators=(",", ":"))


def is_railway_stop(code: str) -> bool:
    return code[1:3] in {"90", "91", "92"} or code == "1930"
