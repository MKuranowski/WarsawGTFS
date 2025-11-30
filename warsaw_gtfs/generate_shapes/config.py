from dataclasses import dataclass

import routx
from impuls import selector
from impuls.tools.types import StrPath


@dataclass
class GraphConfig:
    osm_resource: str
    profile: routx.OsmProfile | routx.OsmCustomProfile
    bbox: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)
    curation_resource: str = ""
    max_stop_to_node_distance: float = 100.0


@dataclass
class GenerateConfig:
    routes: selector.Routes
    shape_id_prefix: str = ""
    overwrite: bool = False


@dataclass
class LoggingConfig:
    task_name: str = "GenerateShapes"
    dump_errors: StrPath | None = None
    clean_error_dir: bool = True
