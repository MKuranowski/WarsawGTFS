# cSpell: words hafas
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from math import inf
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

ROMAN_TO_INT = {
    "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5",
    "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10",
    "": "",
}

STATION_HAFAS_IDS: Dict[str, str] = {
    "4900": "5100067",  # Warszawa Zachodnia
    "7900": "5100065",  # Warszawa Centralna
    "2900": "5100066",  # Warszawa Wschodnia
    "7903": "5100172",  # Warszawa Gdańska
    "1907": "5101981",  # Legionowo
    "2918": "5102637",  # Otwock
    "2914": "5103424",  # Sulejowek-Milosna
    "1907": "5101981",  # Legionowo
    "1908": "5100323",  # Legionowo-Piaski
    "4905": "5100330",  # Pruszkow
}

STA_NAMES: Dict[str, str] = {
    "4900": "W-wa Zachodnia",
    "7900": "W-wa Centralna",
    "2900": "W-wa Wschodnia",
    "7903": "W-wa Gdańska",
    "1907": "Legionowo",
    "2918": "Otwock",
    "2914": "Sulejówek Miłosna",
    "1907": "Legionowo",
    "1908": "Legionowo Piaski",
    "4905": "Pruszków",
}

# station_id, headsign -> platform
FALLBACK_PLATFORMS: Dict[Tuple[str, str], str] = {
    ("2918", "warszawawschodnia"): "2",
    ("2918", "otwock"): "2",
    ("2900", "warszawawschodnia"): "6",
    ("2900", "otwock"): "7",
    ("4905", "pruszków"): "1",
}


_logger = logging.getLogger("WarsawGTFS.Platforms")


@dataclass
class PlatformEntry:
    """Stored entry with platform (and more) data"""
    number: str
    route: str
    headsign: str
    platform: str
    dates: Optional[Set[date]] = None

    def is_similar(self, other: "PlatformEntry") -> bool:
        return self.number == other.number and \
            (self.platform == other.platform or not self.platform or not other.platform)


@dataclass
class PlatformLookupQuery:
    """All required data to search for a matching PlatformEntry"""
    station_id: str
    gtfs_time: str
    route: str
    headsign: str
    train_dates: Set[date]
    calendar_start: date
    is_last: bool
    matched_number: str = ""
    time: int = 0


PlatformEntries = Dict[str, Dict[int, List[PlatformEntry]]]
PlatformFilter = Callable[[PlatformEntry], bool]


class PlatformHandler:
    """Singleton class responsible for loading external railway data"""
    _INSTANCE: Optional["PlatformHandler"] = None

    def __init__(self) -> None:
        if self._INSTANCE:
            raise RuntimeError("Attempt to create a new PlatformHandler")

        self.departures: PlatformEntries = {}
        self.arrivals: PlatformEntries = {}

    def load_data(self) -> None:
        """Loads data from the external API"""
        s = requests.Session()
        self.arrivals.clear()
        self.departures.clear()

        for ztm_id, hafas_id in STATION_HAFAS_IDS.items():
            r = s.get(f"https://mkuran.pl/other/rail-platforms/{hafas_id}.json")
            r.raise_for_status()
            data = r.json()

            self.departures[ztm_id] = {}
            self.arrivals[ztm_id] = {}

            self.load_entries_into(data["arrivals"], ztm_id, self.arrivals[ztm_id])
            self.load_entries_into(data["departures"], ztm_id, self.departures[ztm_id])

    @staticmethod
    def load_entries_into(entries: Any, station_id: str, into: Dict[int, List[PlatformEntry]]) \
            -> None:
        for entry in entries:
            # Only care about SKM Warszawa trains
            if entry["number"][:3] != "SKW":
                continue

            # Parse the time
            h, _, m = entry["time"].partition(":")
            time = int(h) * 3600 + int(m) * 60

            # Create the entry
            into.setdefault(time, [])
            entry = PlatformEntry(
                entry["number"].partition(" ")[2],
                entry["name"],
                entry["headsign"].casefold().replace(" ", ""),
                ROMAN_TO_INT[entry["platform"]],
                {date.fromisoformat(i) for i in entry["only_on_dates"]}
                if entry["only_on_dates"] else None,
            )

            # Check if there's a similar entry - if so, merge the `dates` attribute
            for idx, existing_entry in enumerate(into[time]):
                if existing_entry.is_similar(entry):
                    # A similar entry is found.
                    # Some magic to handle merging of the `dates` attributes
                    if existing_entry.dates is not None and entry.dates is not None:
                        # Both entries are active on specific dates:
                        existing_entry.dates.update(entry.dates)

                    elif entry.dates is None:
                        existing_entry.dates = None

                    # else: existing_entry is active everyday - no merging to do

                    # Also merge the platform attributes if one of the entries is missing one
                    existing_entry.platform = existing_entry.platform or entry.platform

                    # Similar entry was found - break out of the loop
                    break

            else:
                # No similar entry - just insert it
                into[time].append(entry)

    @staticmethod
    def _has_entry(entries: List[PlatformEntry]) -> Tuple[bool, Optional[PlatformEntry]]:
        if not entries:
            return True, None
        elif len(entries) == 1:
            return True, entries[0]
        else:
            return False, None

    @staticmethod
    def _single_result(entries: List[PlatformEntry], *filters: PlatformFilter) \
            -> Optional[PlatformEntry]:
        filtered = [entry for entry in entries if all(filter(entry) for filter in filters)]
        return filtered[0] if len(filtered) == 1 else None

    @staticmethod
    def _scored_result(entries: List[PlatformEntry], calendar_start: date,
                       *filters: PlatformFilter) -> Optional[PlatformEntry]:
        # Scoring function
        def calculate_entry_score(entry: PlatformEntry) -> float:
            if entry.dates is None:
                return -inf  # Calendars active every day are considered best-fit
            try:
                # Generally, the score is the "distance" from calendar_start
                return min((d - calendar_start).days for d in entry.dates if d >= calendar_start)
            except ValueError:
                return inf  # Calendars only active in the past are worst-fit

        filtered = [entry for entry in entries if all(filter(entry) for filter in filters)]
        return min(filtered, key=calculate_entry_score) if filtered else None

    @staticmethod
    def _inexact_result(entries: Dict[int, List[PlatformEntry]], query: PlatformLookupQuery) \
            -> Optional[PlatformEntry]:
        # This method is meant to catch weird edge case where the time between platform data
        # is not exactly the same as the time in ZTM data.
        # Matching however relies on having the train number.
        if not query.matched_number:
            return None

        # Find matching platform_entries for the train number
        entries_with_the_same_number = [
            entry
            for entries_at_time in entries.values()
            for entry in entries_at_time
            if entry.number == query.matched_number
        ]

        if not entries_with_the_same_number:
            return None

        if len(entries_with_the_same_number) == 1:
            return entries_with_the_same_number[0]

        # Last resort - filter by operating dates
        f_dates: PlatformFilter = \
            lambda i: i.dates is None or bool(i.dates.intersection(query.train_dates))
        return PlatformHandler._scored_result(entries_with_the_same_number, query.calendar_start,
                                              f_dates)

    def do_get_entry(self, query: PlatformLookupQuery, dep: bool) -> Optional[PlatformEntry]:
        # We extract all the possible entries, then try to narrow down the possibilities, with
        # the following filters:
        # 1. (no filters)
        # 2. route_id
        # 3. route_id & headsign
        # 4. route_id & headsign & operating_dates
        # 5. route_id & operating_dates
        #
        # If those filters still didn't provide a single match, we try to select
        # an entry closest to calendar_start.
        # This is done on the outputs of the previous steps, in the following order:
        # 1. route_id & headsign & operating_dates
        # 2. route_id & headsign
        # 3. route_id
        #
        # However, there are still some edge cases where we get no matches,
        # due to off-by-one errors in time (e.g. ZTM would publish 24:00:00,
        # but PKP publishes 23:59). As a last resort, we filter by the following keys:
        # 1. trip_short_name & abs(ztm.time - platform.time) < 5 mins
        entries_at_station = (self.departures if dep else self.arrivals).get(query.station_id, {})
        all_entries = entries_at_station.get(query.time, [])

        f_route_id: PlatformFilter = lambda i: i.route == query.route or not i.route
        f_headsign: PlatformFilter = lambda i: i.headsign == query.headsign
        f_dates: PlatformFilter = \
            lambda i: i.dates is None or bool(i.dates.intersection(query.train_dates))

        # beautiful, isn't it :)
        return self._single_result(all_entries) \
            or self._single_result(all_entries, f_route_id) \
            or self._single_result(all_entries, f_route_id, f_headsign) \
            or self._single_result(all_entries, f_route_id, f_headsign, f_dates) \
            or self._single_result(all_entries, f_route_id, f_dates) \
            or self._scored_result(all_entries, query.calendar_start, f_route_id, f_headsign,
                                   f_dates) \
            or self._scored_result(all_entries, query.calendar_start, f_route_id, f_headsign) \
            or self._scored_result(all_entries, query.calendar_start, f_route_id) \
            or self._scored_result(all_entries, query.calendar_start) \
            or self._inexact_result(entries_at_station, query)

    def get_entry(self, query: PlatformLookupQuery) -> Optional[PlatformEntry]:
        if query.station_id not in STATION_HAFAS_IDS:
            return

        # Quick fix for the name of "Warszawa Zachodnia (peron 9)"
        # - the API doesn't return the parenthesis
        query.headsign = query.headsign.casefold() \
            .replace("(", "").replace(")", "").replace(" ", "")

        h, m, s = map(int, query.gtfs_time.split(":"))
        days_offset, h = divmod(h, 24)  # Wrap around past-24 departures
        query.time = (h * 3600 + m * 60 + s)

        # Apply day_offset
        if days_offset:
            delta = timedelta(days=days_offset)
            query.train_dates = {i + delta for i in query.train_dates}

        # Make the query
        result = self.do_get_entry(query, dep=not query.is_last)

        # Try to use the fallback entry
        if result is None and (query.station_id, query.headsign) in FALLBACK_PLATFORMS:
            fallback_platform = FALLBACK_PLATFORMS[query.station_id, query.headsign]
            _logger.error(
                f"No matching platform at {STA_NAMES[query.station_id]}, {query.gtfs_time} "
                f"for {query.route} train to {query.headsign} - "
                f"using {fallback_platform} as a fallback"
            )
            result = PlatformEntry(
                number="",
                route=query.route,
                headsign="",
                platform=fallback_platform,
                dates=None,
            )

        # If the result is still None - throw an error
        if result is None:
            raise ValueError(
                f"No matching platform at {STA_NAMES[query.station_id]}, {query.gtfs_time} "
                f"for {query.route} train to {query.headsign}"
            )

        return result

    @classmethod
    def instance(cls) -> "PlatformHandler":
        if not cls._INSTANCE:
            cls._INSTANCE = cls()
            cls._INSTANCE.load_data()
        return cls._INSTANCE
