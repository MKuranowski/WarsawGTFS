from contextlib import contextmanager
import signal
import math
import os

"""
Generic utility functions.
Either geographical ones, or
utilities that could be used by both static and realtime parsers
"""

@contextmanager
def time_limit(sec):
    "Time limter based on https://gist.github.com/Rabbit52/7449101"
    def handler(x, y): raise TimeoutError
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(sec)
    try: yield
    finally: signal.alarm(0)

def clear_directory(directory):
    if not os.path.exists(directory): os.mkdir(directory)
    for file in [os.path.join(directory, x) for x in os.listdir(directory)]: os.remove(file)

def haversine(pt1, pt2):
    "Calculate haversine distance (in km)"
    lat1, lon1 = map(math.radians, pt1)
    lat2, lon2 = map(math.radians, pt2)
    lat = lat2 - lat1
    lon = lon2 - lon1
    d = math.sin(lat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(lon * 0.5) ** 2
    return 2 * 6371 * math.asin(math.sqrt(d))

def iter_haversine(points):
    "Calculate total route distance"
    total = 0.0
    for i in range(1, len(points)):
        total += haversine(points[i-1], points[i])
    return total

def avg_position(stops_in_group):
    lats = list(map(float, [i[0] for i in stops_in_group.values()]))
    lons = list(map(float, [i[1] for i in stops_in_group.values()]))
    avg_lat = round(sum(lats)/len(lats), 8)
    avg_lon = round(sum(lons)/len(lons), 8)
    return str(avg_lat), str(avg_lon)

def initial_bearing(pos1, pos2):
    "Calculate initial bearing of vehicle, only if the vehicle has moved more than 30m"
    if haversine(pos1, pos2) < 0.003: return None
    lat1, lat2, lon = map(math.radians, [pos1[0], pos2[0], pos2[1] - pos1[1]])
    x = math.sin(lon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(lon))
    return math.degrees(math.atan2(x, y))
