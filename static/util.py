# cSpell: words mkdtemp

import os
import zipfile
from dataclasses import dataclass
from tempfile import mkdtemp
from typing import Any, Iterable, Optional, Protocol, Tuple

import coloredlogs

from .const import LOGGING_FMT, LOGGING_STYLE

"""
Module containing various utility functions
"""


# = TYPE UTILITIES = #

class CsvWriter(Protocol):
    def writerow(self, __row: Iterable[Any]) -> Any: ...


# = DATA UTILITIES = #

@dataclass
class ConversionOpts:
    """Toggles for the whole conversion process"""
    __slots__ = (
        "target", "sync_time", "pub_name", "pub_url", "metro", "shapes", "simplify_shapes", "bus_color", "tram_color", "bus_express_color", "night_bus_color",
        "train_color", "zone_color", "special_color", "supplementary_color", "bus_text_color", "tram_text_color", "bus_express_text_color", "night_bus_text_color",
        "train_text_color", "zone_text_color", "special_text_color", "supplementary_text_color"
    )

    target: str     # Where to put the created .zip file
    sync_time: str  # Time when data was downloaded (for attributions.txt)
    pub_name: str   # value for feed_publisher_name
    pub_url: str    # value for feed_publisher_url
    metro: bool     # whether to add metro schedules
    shapes: bool    # whether to generate shapes
    simplify_shapes: bool  # whether to simplify generated shapes

    bus_color: str
    tram_color: str
    bus_express_color: str
    night_bus_color: str
    train_color: str
    zone_color: str
    special_color: str
    supplementary_color: str

    bus_text_color: str
    tram_text_color: str
    bus_express_text_color: str
    night_bus_text_color: str
    train_text_color: str
    zone_text_color: str
    special_text_color: str
    supplementary_text_color: str

    def get_route_color_type(self, id: str, desc: str) -> Tuple[str, str, str]:
        """Get route_type, route_color, route_text_color based on route's id and description."""
        desc = desc.casefold()
        if "kolei" in desc:
            return "2", self.train_color, self.train_text_color
        elif "tram" in desc:
            return "0", self.tram_color, self.tram_text_color
        elif "specjalna" in desc and id in {"W", "M"}:
            return "0", self.special_color, self.special_text_color
        elif "nocna" in desc:
            return "3", self.night_bus_color, self.night_bus_text_color
        elif "uzupełniająca" in desc:
            return "3", self.supplementary_color, self.supplementary_text_color
        elif "strefowa" in desc:
            return "3", self.zone_color, self.zone_text_color
        elif "ekspresowa" in desc or "przyspieszona" in desc:
            return "3", self.bus_express_color, self.bus_express_text_color
        else:
            return "3", self.bus_color, self.bus_text_color


def normal_time(time: str, lessthen24: bool = False) -> str:
    """Normalizes time from ZTM-file format (H.MM / HH.MM) to GTFS format (HH:MM:SS).
    lessthen24 argument ensures hour will be less then 24.
    """
    h, m = map(int, time.split("."))
    if lessthen24:
        while h >= 24:
            h -= 24
    return f"{h:0>2}:{m:0>2}:00"


def setup_logging(verbose: bool = False) -> None:
    coloredlogs.install(
        level="DEBUG" if verbose else "INFO",
        style=LOGGING_STYLE,
        fmt=LOGGING_FMT
    )


# = FILE SYSTEM UTILITIES = #


def clear_directory(path: str) -> None:
    """Clears the contents of a directory. Only files can reside in this directory."""
    for f in os.scandir(path):
        os.remove(f.path)


def ensure_dir_exists(path: str, clear: bool = False) -> bool:
    """Ensures such given directory exists.
    Returns False if directory was just created, True if it already exists.
    """
    try:
        os.mkdir(path)
        return False
    except FileExistsError:
        if clear:
            clear_directory(path)
        return True


def prepare_tempdir(suffix: Optional[str] = None) -> str:
    """Preapres a temprary directory, and returns the path to it.
    f"_{version}" will be used as the suffix of this directory, if provided.
    """
    suffix = "_" + suffix if suffix else None
    dir_str = mkdtemp(suffix, "warsawgtfs_")
    return dir_str


def compress(directory: str = "gtfs", target: str = "gtfs.zip") -> None:
    """Compress all *.txt files from directory into GTFS named 'target'"""
    with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
        for f in os.scandir(directory):
            if f.name.endswith(".txt"):
                arch.write(f.path, arcname=f.name)
