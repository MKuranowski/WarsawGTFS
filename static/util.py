from tempfile import mkdtemp
from typing import Optional
import coloredlogs
import zipfile
import os


from .const import LOGGING_FMT, LOGGING_STYLE

"""
Module containing various utility functions
"""


# = DATA UTILITIES = #


def normal_time(time, lessthen24=False):
    """Normalizes time from ZTM-file format (H.MM / HH.MM) to GTFS format (HH:MM:SS).
    lessthen24 argument ensures hour will be, less theen 24.
    """
    h, m = map(int, time.split("."))
    if lessthen24:
        while h >= 24:
            h -= 24
    return f"{h:0>2}:{m:0>2}:00"


def setup_logging(verbose: bool = False):
    coloredlogs.install(
        level="DEBUG" if verbose else "INFO",
        style=LOGGING_STYLE,
        fmt=LOGGING_FMT
    )


# = FILE SYSTEM UTILITIES = #


def clear_directory(path: str):
    """Clears the contest of a directory. Only files can reside in this directory."""
    for f in os.scandir(path):
        os.remove(f.path)


def ensure_dir_exists(path: str, clear: bool = False):
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


def compress(directory: str = "gtfs", target: str = "gtfs.zip"):
    """Compress all *.txt files from directory into GTFS named 'target'"""
    with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
        for f in os.scandir(directory):
            if f.name.endswith(".txt"):
                arch.write(f.path, arcname=f.name)
