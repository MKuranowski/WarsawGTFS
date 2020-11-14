from datetime import datetime, date
from typing import Optional
from pytz import timezone

from .downloader import sync_single_file
from .converter import Converter

"""
Module conatins function that coordinate file synchornization with GTFS convertions.
"""


def make_single(for_day: Optional[date] = None, target: str = "gtfs.zip",
                pub_name: str = "", pub_url: str = "", metro: bool = False, shapes: bool = False):
    # Get file
    sync_time = datetime.now(timezone("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S")
    file_info = sync_single_file(for_day)

    # Convert
    Converter.create(file_info, target, sync_time, False, pub_name, pub_url, metro, shapes)
