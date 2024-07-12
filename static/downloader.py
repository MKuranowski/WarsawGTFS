import fnmatch
import ftplib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from operator import itemgetter
from typing import Dict, List, Optional, Set, Tuple

import py7zr
from pytz import timezone

from .const import DIR_CONVERTED, DIR_DOWNLOAD, FTP_ADDR
from .util import ensure_dir_exists

"""
Module contains code responsible for synchornising feeds with the FTP server.
Calculates which feeds need to be converted, gets and decompresses required files.

Information about specific feeds is passed around with the FileInfo objects.
Main functionality is exposed in the sync_feeds() and append_modtimes() procedures.
"""

# cSpell: words retr mlsd

_logger = logging.getLogger("WarsawGTFS.downloader")


@dataclass
class FileInfo:
    """Info about a source file"""
    __slots__ = ("path", "version", "modtime", "start", "end", "is_converted")

    path: str
    version: str
    modtime: str
    start: date
    end: date
    is_converted: bool


def read_modtimes() -> Dict[str, str]:
    """Reads {DIR_CONVERTED}/modtimes.json to determine modtimes of currently converted files.
    """
    modtimes_file = os.path.join(DIR_CONVERTED, "modtimes.json")

    # File doesn't exist: no known files
    if not os.path.isfile(modtimes_file):
        return {}

    with open(str(modtimes_file), mode="r") as f:
        return json.load(f)


def write_modtimes(x: Dict[str, str]) -> None:
    """Writes new content to the {DIR_CONVERTED}/modtimes.json"""
    with open(os.path.join(DIR_CONVERTED, "modtimes.json"), "w") as f:
        json.dump(x, f, indent=2)


def append_modtimes(new: FileInfo) -> None:
    """Once a converted feed has been written to {DIR_CONVERTED}, call this function.
    Adds info about just converted feed to {DIR_CONVERTED}/modtimes.json/
    """
    c = read_modtimes()
    c[new.version] = new.modtime
    write_modtimes(c)


def which_versions_ok(required_feeds: List[FileInfo],
                      current_modtimes: Dict[str, str]) -> Set[str]:
    """Returns a set of versions which don't need to bee re-converted."""
    ok_versions: Set[str] = set()

    for i in required_feeds:
        current_modtime = current_modtimes.get(i.version, "")
        if current_modtime == i.modtime:
            ok_versions.add(i.version)

    return ok_versions


def remove_unused_converted(versions_ok: Set[str], current_modtimes: Dict[str, str]) -> None:
    """Removes outdated and unnecessary feeds from {DIR_CONVERTED},
    updates {DIR_CONVERTED}/modtimes.json
    """
    _logger.info("removing unnecessary files from DIR_CONVERTED")

    # Calculate which files to remove
    all_files = set(os.listdir(DIR_CONVERTED))
    expected_files = {"modtimes.json"}
    expected_files.update(i + ".zip" for i in versions_ok)
    unexpected_files = all_files.difference(expected_files)

    # Schedule removal of unexpected files
    for f in unexpected_files:
        os.remove(os.path.join(DIR_CONVERTED, f))

    # Remove versions from current_modtimes
    new_modtimes = {k: v for k, v in current_modtimes.items() if k in versions_ok}
    write_modtimes(new_modtimes)


def list_files(ftp: ftplib.FTP, max_files: int = 5,
               start_date: Optional[date] = None) -> List[FileInfo]:
    """Lists all files required to create a valid feed.
    At most {max_files} will be returned (defaults to 5).
    Required files are evaulated starting from start_date, which defaults to 'today' in Warsaw.
    """
    _logger.info("calculating required files")
    files = ftp.mlsd()
    fn_match = re.compile(r"^RA\d{6}\.7z")

    # Ignore non-schedule files & sort files by date
    files = sorted(
        filter(lambda i: fn_match.fullmatch(str(i[0])), files),
        key=itemgetter(0)
    )

    # User hasn't specified when the feed should be valid: start from 'today' (in Warsaw)
    if start_date is None:
        start_date = datetime.now(timezone("Europe/Warsaw")).date()

    active_files: List[FileInfo] = []

    # Check which files should be converted
    for idx, (file_name, file_meta) in enumerate(files):
        file_start = datetime.strptime(file_name, "RA%y%m%d.7z").date()

        # Get last day when file is active (next file - 1 day)
        try:
            file_end = datetime.strptime(str(files[idx + 1][0]), "RA%y%m%d.7z").date()
            file_end -= timedelta(days=1)
        except IndexError:
            file_end = date.max

        # We don't need anything for previous dates
        if file_end < start_date:
            continue

        active_files.append(FileInfo(
            path=file_name, version=file_name[:-3], modtime=file_meta["modify"],
            start=file_start, end=file_end, is_converted=False
        ))

    # Limit files to max_files
    active_files = active_files[:max_files]

    # Last file shouldn't have an end_date
    active_files[-1].end = date.max

    return active_files


def list_single_file(ftp: ftplib.FTP, for_day: Optional[date] = None) -> FileInfo:
    """Returns FileInfo about file valid in the given day (or today in Warsaw)"""
    # Variables from loop
    file_name = ""
    file_meta = {}
    file_start = date.min

    # List files from FTP
    files = ftp.mlsd()
    fn_match = re.compile(r"^RA\d{6}\.7z")

    # Ignore non-schedule files & sort files by date
    files = sorted(
        filter(lambda i: fn_match.fullmatch(str(i[0])), files),
        key=itemgetter(0)
    )

    # Ensure for_day is not None
    if for_day is None:
        for_day = datetime.now(timezone("Europe/Warsaw")).date()

    # Iterate over files and to get one valid for `for_day`
    max_idx = len(files) - 1
    for idx, (file_name, file_meta) in enumerate(files):
        file_start = datetime.strptime(file_name, "RA%y%m%d.7z").date()

        # If user requested file before the very first file - raise an error
        if idx == 0 and for_day < file_start:
            raise FileNotFoundError(f"No files for day {for_day.strftime('%Y-%m-%d')}")

        # Last file always matches
        if idx != max_idx:
            next_file_start = datetime.strptime(str(files[idx + 1][0]), "RA%y%m%d.7z").date()

            # Next file starts past for_day, so current file matches - break out of the loop
            if next_file_start > for_day:
                break

    # guard against no matches
    if not file_name:
        raise FileNotFoundError(f"Error matching files for day {for_day.strftime('%Y-%m-%d')}")

    # file_path, file_start and file_meta now contain info about matched file
    return FileInfo(
        path=file_name, version=file_name[:-3], modtime=file_meta["modify"],
        start=file_start, end=date.max, is_converted=False
    )


def get_and_decompress(ftp: ftplib.FTP, i: FileInfo) -> None:
    """Requests given file from the FTP server, and decompresses the included TXT file.

    Provided FileInfo object will be modified as such:
    - finfo.path points to the decompressed txt file
    - finfo.is_converted is False
    """
    # Download the 7z file into DIR_DOWNLOAD
    txt_file_name = i.version + ".TXT"
    archive_local_path = os.path.join(DIR_DOWNLOAD, i.path)
    txt_local_path = os.path.join(DIR_DOWNLOAD, txt_file_name)

    _logger.debug(f"Downloading file for version {i.version}")
    with open(archive_local_path, mode="wb") as f:
        ftp.retrbinary("RETR " + str(i.path), f.write)

    # Open the 7z file and decompress the txt file
    _logger.debug(f"Decompressing file for version {i.version}")
    with py7zr.SevenZipFile(archive_local_path, mode='r') as archive:
        for file in archive.files:
            if file.filename == txt_file_name:
                archive.extract(path=DIR_DOWNLOAD, targets=[txt_file_name])
                break
        else:
            raise FileNotFoundError(f"{txt_file_name} not found in {i.path}")

    # Modify given FileInfo
    assert os.path.exists(txt_local_path)
    i.path = txt_local_path
    i.is_converted = False

    # Remove downloaded archive
    os.remove(archive_local_path)


def mark_as_converted(i: FileInfo) -> None:
    """Modifies given FileInfo object to mark it as already-converted.
    finfo.is_converted will be True, and finfo.path will point to the GTFS .zip.
    """
    i.path = os.path.join(DIR_CONVERTED, (i.version + ".zip"))
    i.is_converted = True


def sync_files(max_files: int = 5, start_date: Optional[date] = None, reparse_all: bool = False) \
        -> Tuple[List[FileInfo], bool]:
    """Manages required source feeds.

    1. Lists required source feeds
    2. Determines which feeds were already converted.
    3. Removes non-required and outdated feeds
    4. Downloads new/changed source feeds amd extracts them.

    Returns 2 values:
    1. List of *all* required feeds
    2. Whether any new files were downloaded

    Please call append_modtimes for each successfully converted file.
    """
    # Ensure DIR_DOWNLOAD and DIR_CONVERTED exist
    ensure_dir_exists(DIR_DOWNLOAD, clear=True)
    ensure_dir_exists(DIR_CONVERTED, clear=False)

    with ftplib.FTP(FTP_ADDR) as ftp:
        ftp.login()

        # List source feeds
        required_feeds = list_files(ftp, max_files, start_date)

        # Determine which feeeds were already converted
        if not reparse_all:
            current_modtimes = read_modtimes()
            converted_versions = which_versions_ok(required_feeds, current_modtimes)
        else:
            current_modtimes = {}
            converted_versions = set()

        # Check if new files will be downloaded
        new_files = sum(1 for i in required_feeds if i.version not in converted_versions)

        # Clean-up {DIR_CONVERTED}
        if new_files > 0:
            _logger.info(f"new files to download&convert: {new_files}")
            remove_unused_converted(converted_versions, current_modtimes)
        else:
            _logger.info("no new files")

        # Download txt files or mark feeds as converted
        for i in required_feeds:
            if i.version in converted_versions:
                mark_as_converted(i)
            else:
                get_and_decompress(ftp, i)

    return required_feeds, (new_files > 0)


def sync_single_file(valid_day: Optional[date] = None) -> FileInfo:
    """Manages required feed to create schedules for given day.
    Downloads and decompresses detected required file.

    Returns the FileInfo object containing data bout the required feed.
    Call append_modtimes after successfully converting this feed.
    """
    # Ensure DIR_DOWNLOAD and DIR_CONVERTED exist
    ensure_dir_exists(DIR_DOWNLOAD, clear=True)

    with ftplib.FTP(FTP_ADDR) as ftp:
        ftp.login()

        # Check which file to download
        finfo = list_single_file(ftp, valid_day)

        # Download said file
        _logger.info(f"Downloading feed for {finfo.version}")
        get_and_decompress(ftp, finfo)

    return finfo
