from pyroutelib3 import distHaversine
from contextlib import contextmanager
from typing import IO, Optional, List, Union, Tuple
from time import time
import signal
import math
import os

from ..const import DIR_SHAPE_CACHE, SHAPE_CACHE_TTL
from ..util import ensure_dir_exists

_Pt = Tuple[float, float]


@contextmanager
def time_limit(sec):
    "Time limter based on https://gist.github.com/Rabbit52/7449101"
    def handler(x, y):
        raise TimeoutError
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(sec)
    try:
        yield
    finally:
        signal.alarm(0)


def total_length(x: List[_Pt]) -> float:
    dist = 0.0
    for i in range(1, len(x)):
        dist += distHaversine(x[i-1], x[i])
    return dist


def dist_point_to_line(r: _Pt, p1: _Pt, p2: _Pt) -> float:
    """Defines distance from point r to line defined by point p1 and p2."""
    # See https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line,
    # algorithm "Line defined by two points"
    # Unpack coordinates
    x0, y0 = r
    x1, y1 = p1
    x2, y2 = p2

    # DIfferences between p1, p2 coordinates
    dx = x2 - x1
    dy = y2 - y1

    return abs(dy*x0 - dx*y0 + x2*y1 - y2*x1) / math.sqrt(dy**2 + dx**2)


def simplify_line(x: List[_Pt], threshold: float) -> List[_Pt]:
    """Simplifies line x using the Ramer-Douglas-Peucker algorithm"""
    # Unable to simplify 2-point lines any further
    if len(x) <= 2:
        return x

    # Find point furthest away from line (x[0], x[-1])
    furthest_pt_dist = 0
    furthest_pt_index = -1

    for pt_idx, pt in enumerate(x[1:-1], start=1):
        pt_dist = dist_point_to_line(pt, x[0], x[-1])
        if pt_dist > furthest_pt_dist:
            furthest_pt_dist = pt_dist
            furthest_pt_index = pt_idx

    # If furthest point is further then given threshold, simplify recursively both parts
    if furthest_pt_dist > threshold:
        left_simplified = simplify_line(x[:furthest_pt_index + 1], threshold)
        right_simplified = simplify_line(x[furthest_pt_index:], threshold)

        # strip last point from `left_simplified` to avoid furthest point being included twice
        return left_simplified[:-1] + right_simplified

    # If furthest point is close then given threshold, the simplification is just the
    # segment from start & end of x.
    else:
        return [x[0], x[-1]]


def cache_retr(file: str, ttl_minutes: int = SHAPE_CACHE_TTL) -> Optional[IO[bytes]]:
    """
    Tries to read specified from cahce.
    If file is older then specified time-to-live,
    or cahced files doesn't exist at all, returns None.
    Otherwise, returns a file-like object.
    """
    file_path = os.path.join(DIR_SHAPE_CACHE, file)

    # Check if cahced file exists
    if not os.path.exists(file_path):
        return

    # Try to get file's last-modified attribute
    file_stat = os.stat(file_path)
    file_timediff = (time() - file_stat.st_mtime) / 60

    # File was modified earlier then specified time-to-live, return a IO object to that file
    if file_timediff < ttl_minutes:
        return open(file_path, "rb")


def cache_save(file: str, reader: Union[IO[bytes], bytes]):
    """Caches contents of `reader` in DIR_SHAPE_CACHE/{file}."""
    ensure_dir_exists(DIR_SHAPE_CACHE, clear=False)
    file_path = os.path.join(DIR_SHAPE_CACHE, file)

    # Check if cahced file exists
    with open(file_path, "wb") as writer:
        if isinstance(reader, bytes):
            writer.write(reader)
        else:
            while (chunk := reader.read(1024 * 16)):
                writer.write(chunk)
