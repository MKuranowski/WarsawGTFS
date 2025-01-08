import json
from typing import Any


def compact_json(data: Any) -> str:
    return json.dumps(data, indent=None, separators=(",", ":"))
