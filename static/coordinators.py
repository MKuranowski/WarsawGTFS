from datetime import datetime, date
from os.path import join
from static.shapes import Shaper
from typing import Optional, Tuple
from pytz import timezone
from copy import copy

from .downloader import append_modtimes, mark_as_converted, sync_files, sync_single_file
from .converter import Converter
from .const import DIR_CONVERTED, DIR_SHAPE_ERR
from .merger import Merger
from .util import ConversionOpts, ensure_dir_exists

"""
Module conatins function that coordinate file synchornization with GTFS convertions.
"""


def make_single(opts: ConversionOpts, for_day: Optional[date] = None) -> str:
    # Get file
    opts.sync_time = datetime.now(timezone("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S")
    file_info = sync_single_file(for_day)

    # Convert
    Converter.create(file_info, opts)

    return file_info.version


def make_multiple(
        opts: ConversionOpts,
        for_day: Optional[date] = None,
        max_files: int = 5,
        force_reparse: bool = False,
        force_remerge: bool = False) -> Tuple[bool, str]:
    # Get files
    opts.sync_time = datetime.now(timezone("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S")
    files, changed = sync_files(max_files, for_day, force_reparse)

    all_versions = "/".join(i.version for i in files)

    if not (changed or force_remerge):
        return False, all_versions

    files_to_convert = [i for i in files if not i.is_converted] if changed else []

    # Convert files if some should be converted
    if changed:
        # Create a shaper object
        if opts.shapes:
            # Clear shape errors
            ensure_dir_exists(DIR_SHAPE_ERR, True)
            shaper = Shaper()
        else:
            shaper = None

        for file in files_to_convert:
            file_opts = copy(opts)
            file_opts.metro = False
            file_opts.target = join(DIR_CONVERTED, (file.version + ".zip"))

            Converter.create(
                file,
                file_opts,
                in_temp_dir=False,
                shaper_obj=shaper,
                clear_shape_errors=False)

            mark_as_converted(file)
            append_modtimes(file)

    # Merge feeds
    Merger.create(files, opts, in_temp_dir=False)

    return True, all_versions
