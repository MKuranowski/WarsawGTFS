import csv
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from io import BytesIO, TextIOWrapper
from logging import getLogger
from os.path import exists, join
from typing import IO, Dict, Generator, List, Optional, Sequence, Set, Tuple
from zipfile import ZipFile

import requests

from .const import URL_METRO_GTFS

"""
Module responsible for appending metro schedules.
"""


# = Helpers = #

def peek_csv_header(file_path: str) -> List[str]:
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header


@contextmanager
def remote_zipfile(url: str) -> Generator[ZipFile, None, None]:
    with requests.get(url) as metro_req:
        metro_req.raise_for_status()
        with BytesIO(metro_req.content) as metro_buff, ZipFile(metro_buff) as metro_arch:
            yield metro_arch


# = Calendar Handling = #

def read_calendars(buffer: IO[str], calendars: Dict[str, List[str]]) \
        -> Tuple[Dict[str, List[str]], date, date]:
    """Reads all calendars from a file-like calendar_dates.txt object into provided dictionary.
    Returns that dictionary, start_date and end_date.
    """
    min_day: date = date.max
    max_day: date = date.min
    reader = csv.DictReader(buffer)

    for row in reader:
        day = datetime.strptime(row["date"], "%Y%m%d").date()

        # Check if day is smaller then current min_day
        if day < min_day:
            min_day = day

        # Check if day is bigger the current max_day
        if day > max_day:
            max_day = day

        # Add to `calendar` dict
        if row["date"] not in calendars:
            calendars[row["date"]] = [row["service_id"]]
        else:
            calendars[row["date"]].append(row["service_id"])

    return calendars, min_day, max_day


def write_calendars(target_dir: str, calendars: Dict[str, List[str]],
                    start_day: date, end_day: date) -> Set[str]:
    """Exports data from `calendars` dict to target_dir/calendar_dates.txt.
    Only dates between (and including) start_day and end_day are considered.
    Returns a set of all exported service_ids.
    """
    target_file = join(target_dir, "calendar_dates.txt")
    used_services: Set[str] = set()

    # Open the file
    with open(target_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "service_id", "exception_type"])

        # Iterate over each day in provided period
        while start_day <= end_day:
            date_str = start_day.strftime("%Y%m%d")

            # Export all services
            for service in calendars[date_str]:
                used_services.add(service)
                writer.writerow([date_str, service, "1"])

            start_day += timedelta(days=1)

    return used_services


def rewrite_calendars(metro_arch: ZipFile, gtfs_dir: str) -> Set[str]:
    """Appends metro calendars to gtfs files in `gtfs_dir` directory.
    Returns a set of all valid service_ids.
    """

    calendars: Dict[str, List[str]] = {}

    # Load calendars from gtfs_dir
    with open(join(gtfs_dir, "calendar_dates.txt"), "r", encoding="utf-8", newline="") as f:
        calendars, local_start, local_end = read_calendars(f, calendars)

    # Load calendars from metro
    with metro_arch.open("calendar_dates.txt", "r") as binary_buff, \
            TextIOWrapper(binary_buff, encoding="utf-8", newline="") as txt_buff:
        calendars, metro_start, metro_end = read_calendars(txt_buff, calendars)

    # Ensure local date-coverage is a sub-section of day's covered by metro schedules
    if metro_start > local_start:
        raise ValueError(
            "Metro schedule start after local ZTM schedule starts"
            f"(metro: {metro_start.strftime('%Y-%m-%d')} vs "
            f"local: {local_start.strftime('%Y-%m-%d')})"
        )

    if metro_end < local_end:
        raise ValueError(
            "Metro schedule ends before local ZTM schedule ends"
            f"(metro: {metro_end.strftime('%Y-%m-%d')} vs "
            f"local: {local_end.strftime('%Y-%m-%d')})"
        )

    # Write combined calendars
    used_services = write_calendars(gtfs_dir, calendars, local_start, local_end)

    return used_services


# = Other File Rewrite = #

def append_routes(metro_arch: ZipFile, gtfs_dir: str) -> List[str]:
    """Appends data from metro_arch zip file to gtfs_dir/filename.

    Returns a list of all inserter route_ids
    """
    filename = "routes.txt"
    inserted_routes = []
    local_file = join(gtfs_dir, filename)

    # Get the header of local GTFS file
    header = peek_csv_header(local_file)

    # Open local file in read+write mode, open file form metro GTFS and create wrap it into text
    with open(local_file, "a", encoding="utf-8", newline="") as target_buff, \
            metro_arch.open(filename, "r") as in_binary_buff, \
            TextIOWrapper(in_binary_buff, encoding="utf-8", newline="") as in_txt_buff:

        # Pass file objects into csv readers/writers.
        reader = csv.DictReader(in_txt_buff)
        writer = csv.DictWriter(target_buff, fieldnames=header, extrasaction="ignore")

        for row in reader:
            # Collect route_id
            inserted_routes.append(row["route_id"])

            # Pass row to writer
            writer.writerow(row)

        target_buff.flush()

    return inserted_routes


def append_from_metro(metro_arch: ZipFile, gtfs_dir: str, filename: str,
                      collect_saved_keys: Optional[Sequence[str]] = None,
                      filter_key: Optional[str] = None, filter_values: Set[str] = set()) \
        -> List[Set[str]]:
    """Appends data from metro_arch zip file to gtfs_dir/filename.

    If `collect_saved_key` is provided, for each saved row `row[collect_saved_key]` will be
    saved in a set. This set will be then returned.

    If `filter_key` is provided, rows for which `row[filter_key] not in filter_values`
    will be skipped.
    """
    collected_keys: List[Set[str]] = [set() for _ in (collect_saved_keys or [])]

    local_file = join(gtfs_dir, filename)

    # Get the header of local GTFS file
    header = peek_csv_header(local_file)

    # Open local file in append mode, open file form metro GTFS and create wrap it into text IO.
    with open(local_file, "a", encoding="utf-8", newline="") as target_buff, \
            metro_arch.open(filename, "r") as in_binary_buff, \
            TextIOWrapper(in_binary_buff, encoding="utf-8", newline="") as in_txt_buff:
        # Pass file objects into csv readers/writers.
        reader = csv.DictReader(in_txt_buff)
        writer = csv.DictWriter(target_buff, fieldnames=header, extrasaction="ignore")

        for row in reader:
            # Check against provided filter
            if filter_key is not None and row[filter_key] not in filter_values:
                continue

            # Set special values
            if filename == "trips.txt" and not row.get("exceptional", ""):
                row["exceptional"] = "0"

            if filename == "routes.txt":
                row["agency_id"] = "0"

            # Collect primary keys
            if collect_saved_keys:
                for idx, key in enumerate(collect_saved_keys):
                    collected_keys[idx].add(row[key])

            # Pass row to writer
            writer.writerow(row)

    return collected_keys


def copy_from_metro(metro_arch: ZipFile, gtfs_dir: str, filename: str,
                    filter_key: Optional[str] = None, filter_values: Set[str] = set()) -> None:
    """If `filter_key` is None, extracts `filename` from metro_arch to gtfs_dir.
    Otherwise, only rows where `row[filter_key] not in filter_values` are skipped.
    """

    # Shortcut if no filter was provided: just extarct the file
    if filter_key is None:
        metro_arch.extract(filename, gtfs_dir)
        return

    # Open local file in write mode, open file form metro GTFS and create wrap it into text IO.
    local_file = join(gtfs_dir, filename)
    with open(local_file, "w", encoding="utf-8", newline="") as target_buff, \
            metro_arch.open(filename, "r") as in_binary_buff, \
            TextIOWrapper(in_binary_buff, encoding="utf-8", newline="") as in_txt_buff:

        # Create the reader and get CSV header from it
        reader = csv.DictReader(in_txt_buff)

        header = reader.fieldnames
        assert header is not None

        # Create the writer
        writer = csv.DictWriter(target_buff, fieldnames=header, extrasaction="ignore")
        writer.writeheader()

        # Re-write each row
        for row in reader:
            # Check against provided filter
            if filter_key is not None and row[filter_key] not in filter_values:
                continue

            # Pass row to writer
            writer.writerow(row)


# = Main function = #

def append_metro_schedule(gtfs_dir: str) -> List[str]:
    logger = getLogger("WarsawGTFS.metro")

    logger.info("Downloading metro GTFS")
    with remote_zipfile(URL_METRO_GTFS) as metro_arch:
        logger.info("Appending metro schedules")

        # Simple files to append
        logger.debug("Appending routes.txt")
        added_routes = append_routes(metro_arch, gtfs_dir)

        logger.debug("Appending stops.txt")
        append_from_metro(metro_arch, gtfs_dir, "stops.txt")

        # Files where some filtering sets need to be extracted from
        logger.debug("Merging calendar_dates.txt")
        valid_services = rewrite_calendars(metro_arch, gtfs_dir)

        logger.debug("Appending trips.txt")
        valid_trips, valid_shapes = append_from_metro(
            metro_arch,
            gtfs_dir,
            "trips.txt",
            collect_saved_keys=["trip_id", "shape_id"],
            filter_key="service_id",
            filter_values=valid_services
        )

        # Rewrite files with filters
        logger.debug("Appending stop_times.txt")
        append_from_metro(metro_arch, gtfs_dir, "stop_times.txt",
                          filter_key="trip_id", filter_values=valid_trips)

        logger.debug("Appending frequencies.txt")
        copy_from_metro(metro_arch, gtfs_dir, "frequencies.txt",
                        filter_key="trip_id", filter_values=valid_trips)

        # Check if shapes.txt already exists
        if exists(join(gtfs_dir, "shapes.txt")):
            shapes_copier = append_from_metro
        else:
            shapes_copier = copy_from_metro

        logger.debug("Appending shapes.txt")
        shapes_copier(metro_arch, gtfs_dir, "shapes.txt",
                      filter_key="shape_id", filter_values=valid_shapes)

    return added_routes
