import csv
import json
from functools import lru_cache
from logging import getLogger
from os.path import join
from tempfile import NamedTemporaryFile
from typing import (Any, Callable, Dict, Iterable, List, Optional, Sequence,
                    Set, Tuple)

import requests

from ..const import (GIST_MISSING_STOPS, GIST_STOP_NAMES, HEADERS,
                     RAIL_STATION_ID_MIDDLES, RAILWAY_MAP)
from ..parser.dataobj import ZTMStop, ZTMStopGroup
from .rail_stations import RailwayStation, RailwayStationLoader

"""
Module reposible for handling handling stop data.

Converts ZTM group-stake hierarchy to GTFS representations of real-life structures.
Fills missing data from external gists (see ../const.py); manages which stops to export,
and all that kind of jazz.
"""


def normalize_stop_name(name: str) -> str:
    """Attempts to fix stop names provided by ZTM"""
    # add .title() if ZTM provides names in ALL-UPPER CASE again
    name = name.replace(".", ". ")      \
               .replace("-", " - ")     \
               .replace("  ", " ")      \
               .replace("al.", "Al.")   \
               .replace("pl.", "Pl.")   \
               .replace("os.", "Os.")   \
               .replace("ks.", "Ks.")   \
               .replace("św.", "Św.")   \
               .replace("Ak ", "AK ")   \
               .replace("Ch ", "CH ")   \
               .replace("gen.", "Gen.") \
               .replace("rondo ", "Rondo ") \
               .replace("most ", "Most ") \
               .rstrip()

    return name


def should_town_be_added_to_name(group: ZTMStopGroup) -> bool:
    """Checks whether town name should be added to the stop name"""
    # List of conditions that, if true, mean town name shouldn't be added
    do_not_add_conditions: Set[Callable[[ZTMStopGroup], bool]] = {
        lambda g: g.town_code == "--",  # Stops in Warsaw
        lambda g: g.id[1:3] in RAIL_STATION_ID_MIDDLES,  # Railway stations
        lambda g: "PKP" in g.name,  # Stops near train stations
        lambda g: "WKD" in g.name,  # Stops near WKD stations
        lambda g: g.town.casefold() in g.name.casefold(),  # Town name is already in stop name

        # Any part of town name is already in the stop name
        lambda g: any(part in g.name.casefold() for part in g.town.casefold().split(" "))
    }

    # Check if all do_not_add_conditions fail
    return not any(rule(group) for rule in do_not_add_conditions)


def avg_position(stops: Sequence[ZTMStop]) -> Optional[Tuple[float, float]]:
    """Returns the average position of all stops"""
    # cSpell: word lons
    lats = (i.lat for i in stops if i.lat is not None)
    lons = (i.lon for i in stops if i.lon is not None)
    count = len(stops)

    if count < 1:
        return None

    return sum(lats) / count, sum(lons) / count


@lru_cache(maxsize=None)
def get_missing_stops() -> Dict[str, Tuple[float, float]]:
    """Gets positions of stops from external gist, as ZTM sometimes omits stop coordinates"""
    with requests.get(GIST_MISSING_STOPS) as req:
        req.raise_for_status()
        return req.json()


@lru_cache(maxsize=None)
def get_rail_platforms() -> Dict[str, RailwayStation]:
    """Gets info about railway stations from external gist"""
    with NamedTemporaryFile(mode="r+b") as f:
        # Download the map
        with requests.get(RAILWAY_MAP, stream=True) as req:
            req.raise_for_status()
            for chunk in req.iter_content():
                f.write(chunk)

        # Load the data
        f.seek(0)
        return RailwayStationLoader.load_all(f)


@lru_cache(maxsize=None)
def get_stop_names() -> Dict[str, str]:
    """Gets fixed stop names for some of the groups"""
    with requests.get(GIST_STOP_NAMES) as req:
        req.raise_for_status()
        return req.json()


class StopHandler:
    def __init__(self, version: str) -> None:
        self.logger = getLogger(f"WarsawGTFS.{version}.StopHandler")

        # Stop data
        self.names: Dict[str, str] = {}
        self.data: Dict[str, Dict[str, Any]] = {}
        self.parents: Dict[str, str] = {}
        self.zones: Dict[str, str] = {}

        # Invalid stop data
        self.invalid: Dict[str, ZTMStop] = {}
        self.change: Dict[str, Optional[str]] = {}

        # Used stops
        self.used_invalid: Set[str] = set()
        self.used: Set[str] = set()

        # External data
        self.missing_stops: Dict[str, Tuple[float, float]] = {}
        self.rail_platforms: Dict[str, RailwayStation] = {}
        self._load_external()

    def _load_external(self) -> None:
        """Loads data from external gists"""
        self.logger.info("Loading data from external gists")
        self.missing_stops = get_missing_stops()
        self.rail_platforms = get_rail_platforms()
        self.names = get_stop_names()

    @staticmethod
    def _match_virtual(virtual: ZTMStop, stakes: Iterable[ZTMStop]) -> Optional[str]:
        """Try to find a normal stake corresponding to given virtual stake"""
        # Find normal stakes with matching position
        with_same_pos: List[str] = []
        if virtual.lat is not None and virtual.lon is not None:
            with_same_pos = [i.id for i in stakes if i.code[0] != "8"
                             and i.lat == virtual.lat and i.lon == virtual.lon]

        # Find normal stakes with matching code
        with_same_code = [i.id for i in stakes if i.code[0] != "8"
                          and i.code[1] == virtual.code[1]]

        # Special Case: Metro Młociny 88 → Metro Młociny 28
        if virtual.id == "605988" and "605928" in with_same_code:
            return "605928"

        # Matched stakes with the same position
        if with_same_pos:
            return with_same_pos[0]

        # Matched stakes with the same code
        elif with_same_code:
            return with_same_code[0]

        # Unable to find a match
        else:
            return None

    def _find_missing_positions(self, stops: List[ZTMStop]) -> None:
        """Matches data from missing_stops to a list of loaded ZTMStops."""
        for idx, stop in enumerate(stops):

            if stop.lat is None or stop.lon is None:
                missing_pos = self.missing_stops.get(stop.id)

                if missing_pos:
                    stops[idx].lat, stops[idx].lon = missing_pos

    def _load_normal_group(self, group_name: str, stops: List[ZTMStop]) -> None:
        """Saves info about normal stop group"""
        for stop in stops:

            # Fix virtual stops
            if stop.code[0] == "8":
                change_to = self._match_virtual(stop, stops)

                if change_to is not None:
                    self.change[stop.id] = change_to

                else:
                    self.invalid[stop.id] = stop

                continue

            # Handle undefined stop positions
            if stop.lat is None or stop.lon is None:
                self.invalid[stop.id] = stop
                continue

            # Save stake into self.data
            self.data[stop.id] = {
                "stop_id": stop.id,
                "stop_name": group_name + " " + stop.code,
                "stop_lat": stop.lat,
                "stop_lon": stop.lon,
                "wheelchair_boarding": stop.wheelchair,
            }

    def _load_railway_group(self, group_id: str, group_name: str,
                            virtual_stops: List[ZTMStop]) -> None:
        """Saves data about a stop group representing a railway station"""
        # Load station info
        station = self.rail_platforms.get(group_id)

        # If this station has no external data - throw an error
        if not station:
            raise ValueError(f"Missing railway station data for {group_id} ({group_name})")

        unmatched_stakes: set[str] = set(stake.id for stake in virtual_stops)

        # Add hub entry
        self.data[group_id] = {
            "stop_id": group_id,
            "stop_name": station.name,
            "stop_lat": station.lat,
            "stop_lon": station.lon,
            "location_type": "1",
            "parent_station": "",
            "stop_IBNR": station.ibnr,
            "stop_PKPPLK": station.pkpplk,
            "wheelchair_boarding": station.wheelchair,
        }

        # Platforms
        for platform in station.platforms:
            platform_id = f"{group_id}p{platform.name}"
            platform_name = f"{station.name} peron {platform.name}"

            # Add platform entry
            self.data[platform_id] = {
                "stop_id": platform_id,
                "stop_name": platform_name,
                "stop_lat": platform.lat,
                "stop_lon": platform.lon,
                "location_type": "0",
                "parent_station": group_id,
                "stop_IBNR": station.ibnr,
                "stop_PKPPLK": station.pkpplk,
                "wheelchair_boarding": platform.wheelchair,
                "platform_code": platform.name,
            }

            # Add to self.parents
            self.parents[platform_id] = group_id

            # Map ZTM stake IDs to this platform
            if platform.ztm_codes:
                for ztm_code in platform.ztm_codes:
                    unmatched_stakes.discard(ztm_code)
                    self.change[ztm_code] = platform_id

        # Special rule to match all stakes to a sole platform
        if len(station.platforms) == 1:
            sole_platform_id = f"{group_id}p{station.platforms[0].name}"
            for ztm_stake in virtual_stops:
                unmatched_stakes.discard(ztm_stake.id)
                self.change[ztm_stake.id] = sole_platform_id

        # Create a fake "unknown" platform
        if unmatched_stakes:
            unknown_platform_id = f"{group_id}pUnknown"
            self.data[unknown_platform_id] = {
                "stop_id": unknown_platform_id,
                "stop_name": station.name,
                "stop_lat": station.lat,
                "stop_lon": station.lon,
                "location_type": "0",
                "parent_station": group_id,
                "stop_IBNR": station.ibnr,
                "stop_PKPPLK": station.pkpplk,
            }

            for unmatched_stake in unmatched_stakes:
                self.change[unmatched_stake] = unknown_platform_id

            self.parents[unknown_platform_id] = group_id

    def load_group(self, group: ZTMStopGroup, stops: List[ZTMStop]) -> None:
        """Loads info about stops of a specific group"""
        # Fix name "Kampinoski Pn" town name
        if group.town == "Kampinoski Pn":
            group.town = "Kampinoski PN"

        # Fix group name
        group.name = normalize_stop_name(group.name)

        # Add town name to stop name & save name to self.names
        if (fixed_name := self.names.get(group.id)):
            group.name = fixed_name

        elif should_town_be_added_to_name(group):
            group.name = group.town + " " + group.name
            self.names[group.id] = group.name

        else:
            self.names[group.id] = group.name

        # Add missing positions to stakes
        self._find_missing_positions(stops)

        # Parse stakes
        if group.id[1:3] in RAIL_STATION_ID_MIDDLES:
            self._load_railway_group(group.id, group.name, stops)

        else:
            self._load_normal_group(group.name, stops)

    def get_id(self, original_id: Optional[str], known_railway_platform: Optional[str] = None) \
            -> Optional[str]:
        """
        Should the stop_id be changed, provide the correct stop_id.
        If given stop_id has its position undefined returns None.
        """
        if original_id is None:
            return None

        # Special case for explicit railway platforms
        if known_railway_platform:
            group_id = original_id[:4]
            platform_id = f"{group_id}p{known_railway_platform}"
            if platform_id not in self.data:
                raise ValueError(f"Missing platform {known_railway_platform!r} at station"
                                 f"{group_id} ({self.data[group_id]['stop_name']})")
            return platform_id

        valid_id = self.change.get(original_id, original_id)

        if valid_id is None:
            return None

        elif valid_id in self.invalid:
            self.used_invalid.add(valid_id)
            return None

        elif valid_id not in self.data:
            assert valid_id[1:3] in RAIL_STATION_ID_MIDDLES, \
                "not loaded stakes should only happen for railway stations"

            raise ValueError(f"Unmapped ZTM code {valid_id} at a railway station "
                             f"({self.data[valid_id[:4]]['stop_name']})")

        else:
            return valid_id

    def use(self, stop_id: str) -> None:
        """Mark provided GTFS stop_id as used"""
        # Check if this stop belongs to a larger group
        parent_id = self.parents.get(stop_id)

        # Mark the parent as used
        if parent_id is not None:
            self.used.add(parent_id)

        self.used.add(stop_id)

    def zone_set(self, group_id: str, zone_id: str) -> None:
        """Saves assigned zone for a particular stop group"""
        current_zone = self.zones.get(group_id)

        # Zone has not changed: skip
        if current_zone == zone_id:
            return

        if current_zone is None:
            self.zones[group_id] = zone_id

        # Boundary stops shouldn't generate a zone conflict warning
        elif current_zone == "1/2" or zone_id == "1/2":
            self.zones[group_id] = "1/2"

        else:
            self.logger.warn(
                f"Stop group {group_id} has a zone conflict: it was set to {current_zone!r}, "
                f"but now it needs to be set to {zone_id!r}"
            )

            self.zones[group_id] = "1/2"

    def export(self, gtfs_dir: str) -> None:
        """Exports all used stops (and their parents) to {gtfs_dir}/stops.txt"""
        # Export all stops
        self.logger.info("Exporting stops")
        with open(join(gtfs_dir, "stops.txt"), mode="w", encoding="utf8", newline="") as f:
            writer = csv.DictWriter(f, HEADERS["stops.txt"])
            writer.writeheader()

            for stop_id, stop_data in self.data.items():
                # Check if stop was used or (is a part of station and not a stop-child)
                if stop_id in self.used or (stop_data.get("parent_station") in self.used
                                            and stop_data.get("location_type") != "0"):

                    # Set the zone_id
                    if not stop_data.get("zone_id"):
                        zone_id = self.zones.get(stop_id[:4])

                        if zone_id is None:
                            self.logger.warn(
                                f"Stop group {stop_id[:4]} has no zone_id assigned (using '1/2')"
                            )
                            zone_id = "1/2"

                        stop_data["zone_id"] = zone_id

                    writer.writerow(stop_data)

        # Calculate unused entries from missing_stops.json
        unused_missing = set(self.missing_stops.keys()) \
            .difference(self.used_invalid) \
            .difference(self.used)

        # Dump missing stops info
        self.logger.info("Exporting missing_stops.json")
        with open("missing_stops.json", "w") as f:
            json.dump(
                {"missing": sorted(self.used_invalid), "unused": sorted(unused_missing)},
                f,
                indent=2
            )
