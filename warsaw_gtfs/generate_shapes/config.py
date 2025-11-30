from dataclasses import dataclass

import routx
from impuls import selector
from impuls.tools.types import StrPath

from .model import BBox


@dataclass
class GraphConfig:
    """Configures *how* the GenerateShapes task generates shapes."""

    osm_resource: str
    """Name of the resource containing OSM data with the routing graph."""

    profile: routx.OsmProfile | routx.OsmCustomProfile
    """Routx profile for parsing the OSM data."""

    bbox: BBox = (0.0, 0.0, 0.0, 0.0)
    """BBox for trimming the OSM data. Useful when osm_resource is a geofabrik data dump,
    in order to reduce memory usage.

    Defaults to (0, 0, 0, 0); which disables the bounding box.
    """

    curation_resource: str = ""
    """Name of a resource containing manual overrides for shape generation"""

    max_stop_to_node_distance: float = 100.0
    """Max distance between a stop and its matched node in the graph."""


@dataclass
class GenerateConfig:
    """Configures *what* the GenerateShapes task generates."""

    routes: selector.Routes
    """Filter for routes for which the task generates shapes."""

    shape_id_prefix: str = ""
    """Prefix for the assigned, numeric shape_ids.
    Otherwise, the generated shapes will have ids as consequential numbers; which might
    conflict with whatever's in the database.
    """

    overwrite: bool = False
    """Overwrite any existing shapes.

    Defaults to False, where any selected trips with shapes will be skipped.
    """


@dataclass
class LoggingConfig:
    """Configures how the GenerateShapes task communicates errors."""

    task_name: str = "GenerateShapes"
    """Name of the task, used for logging."""

    dump_errors: StrPath | None = None
    """Directory for dumping shape generation failures in GeoJSON format.

    Helps in debugging why certain stop pairs were connected by a straight line,
    instead of an actual shape.
    """

    clean_error_dir: bool = True
    """Whether the directory selected by `dump_errors` should be cleaned before running."""
