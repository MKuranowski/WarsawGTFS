# cSpell: words mkdtemp

import os
import zipfile
from dataclasses import dataclass
from tempfile import mkdtemp
from typing import Any, Iterable, Optional, Protocol

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
        "target", "sync_time", "pub_name", "pub_url", "metro", "shapes", "simplify_shapes",
    )

    target: str     # Where to put the created .zip file
    sync_time: str  # Time when data was downloaded (for attributions.txt)
    pub_name: str   # value for feed_publisher_name
    pub_url: str    # value for feed_publisher_url
    metro: bool     # whether to add metro schedules
    shapes: bool    # whether to generate shapes
    simplify_shapes: bool  # whether to simplify generated shapes


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


def is_railway_station(id: str) -> bool:
    """Returns True if the provided stop/stop group ID represents a railway station"""
    return id[1:3] in {"90", "91", "92"} or id[:4] in {"1930"}
