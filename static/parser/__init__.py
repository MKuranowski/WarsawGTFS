from datetime import datetime
from logging import getLogger
from typing import Iterator, Dict, Literal, Protocol
import re

from ..util import normal_time
from .dataobj import (
    ZTMCalendar, ZTMStopGroup, ZTMStop, ZTMStopTime, ZTMTrip,
    ZTMRouteVariant, ZTMVariantStop, ZTMTTableDep, ZTMRoute
)

"""
A set of generators which return nice data from ZTM File.
As all should use one file object, please make sure
to exhaust the generator fully (no break statements, please).

Also, please be sure to follow the nesting and order of sections:
parser = Parser(reader)
for i in parser.parse_ka(): ...
for i in parser.parse_zp():
    for j in parser.parser_pr(): ...
for i in parser.parse_ll():
    for j in parser.parse_tr():
        for k in parser.parse_lw(): ...
        for k in parser.parse_wgod(): ...
    for j in parser.parse_wk(): ...
parser.close()
"""


def _remove_non_digits(text: str) -> str:
    """Removes non-digit charachters from text"""
    result = ""
    for chr in text:
        if chr.isdigit():
            result += chr
    return result


class _WithReadline(Protocol):
    def readline(self) -> str:
        ...


class Parser:
    def __init__(self, reader: _WithReadline, version: str):
        self.r = reader
        self.logger = getLogger(f"WarsawGTFS.{version}.Parser")

    def parse_ka(self) -> Iterator[ZTMCalendar]:
        """
        Skips to section KA and parses data from there.
        Yields ZTMCalendar objects.
        """
        # regexp = re.compile(r"(\d{4})-(\d{2})-(\d{2})\s+\d+\s+([\w\s]+)")
        self.skip_to_section("KA")

        while (line := self.r.readline()):
            line = line.strip()

            # section end
            if "#KA" in line:
                return

            # regex for KA
            line_split = line.split()

            if len(line_split) < 3:
                continue

            # data conversion
            # pylint: disable = no-member
            row_date = datetime.strptime(line_split[0], "%Y-%m-%d").date()

            yield ZTMCalendar(row_date, line_split[2:])

        raise EOFError("End of section KA not reched before EOF!")

    def parse_zp(self) -> Iterator[ZTMStopGroup]:
        """
        Skips to section ZP and parses data from there.
        Yields ZTMStopGroup objects.
        """
        regexp = re.compile(r"(\d{4})\s+([^,]{1,30})[\s,]+([\w-]{2})\s+(.*)")
        self.skip_to_section("ZP")

        while (line := self.r.readline()):
            line = line.strip()

            # section end
            if "#ZP" in line:
                return

            # regex for ZP
            line_match = regexp.match(line)

            if not line_match:
                continue

            # combine data
            yield ZTMStopGroup(
                id=line_match[1], name=line_match[2],
                town=line_match[4].title(), town_code=line_match[3],
            )

        raise EOFError("End of ZP section was not reached before EOF!")

    def parse_pr(self) -> Iterator[ZTMStop]:
        """
        Skips to next PR section and parses data from there.
        Yields ZTMStop objects.
        """
        regexp = re.compile(r"(\d{4})(\d{2}).+Y=\s?([0-9Yy.]+)\s+X=\s?([0-9Xx.]+)"
                            r"(?:\s+Pu=([0-9?]))?")
        self.skip_to_section("PR")

        while (line := self.r.readline()):
            line = line.strip()

            # Section end
            if "#PR" in line:
                return

            # regex for matching data of a stake inside a group
            line_match = regexp.match(line)

            if not line_match:
                continue

            # parse data

            # convert accessibility info → GTFS
            accessibility_data = line_match[5]
            if accessibility_data == "?" or accessibility_data is None:
                wheelchair = "0"
            elif int(accessibility_data) > 5:
                wheelchair = "2"
            else:
                wheelchair = "1"

            # convert stop poisition
            if "y" in line_match[3].lower() or "x" in line_match[4].lower():
                lat = None
                lon = None

            else:
                lat = float(line_match[3])
                lon = float(line_match[4])

            yield ZTMStop(
                id=(line_match[1] + line_match[2]), code=line_match[2],
                lat=lat, lon=lon, wheelchair=wheelchair,
            )

        raise EOFError("End of PR section was not reached before EOF!")

    def parse_wk(self, route_id: str) -> Iterator[ZTMTrip]:
        """
        Skips to next WK section and parses data from there.
        Yields ZTMTrip objects.
        """
        # regexp = re.compile(r"(\S{1,17})\s+(\d{6})\s+\w{2}\s+([0-9.]+)(\s+\w|)")
        self.skip_to_section("WK")

        trip = ZTMTrip(id="", stops=[])

        while (line := self.r.readline()):
            line = line.strip()

            # section end
            if "#WK" in line:

                # yield last trip
                if trip.id and trip.stops:
                    yield trip

                return

            # regex for wk
            line_split = line.split()

            if len(line_split) < 4:
                continue

            flags = line_split[4] if len(line_split) >= 5 else ""

            # data conversion
            stopt = ZTMStopTime(
                stop=line_split[1],
                original_stop=line_split[1],
                time=normal_time(line_split[3]),
                flags=flags,  # type: ignore
            )

            trip_id = route_id + "/" + line_split[0]

            # change of active trip
            if trip.id != trip_id:
                # yield only trips with id and some stops
                if trip.id and trip.stops:
                    yield trip

                trip = ZTMTrip(id=trip_id, stops=[])

            # append stop to active trip
            trip.stops.append(stopt)

        raise EOFError("End of section WK not reched before EOF!")

    def parse_tr(self) -> Iterator[ZTMRouteVariant]:
        """
        Skips to next TR section and parses data from there.
        Yields ZTMRouteVariant objects.
        """
        regexp = re.compile(
            r"([\w-]+)\s*,\s+([^,]{1,30})[\s,]+([\w-]{2})\s+==>\s"
            r"+([^,]{1,30})[\s,]+([\w-]{2})\s+Kier\. (\w)\s+Poz. (\w)"
        )
        self.skip_to_section("TR")

        while (line := self.r.readline()):
            line = line.strip()

            # section end
            if "#TR" in line:
                return

            # regex for TR
            line_match = regexp.match(line)

            if not line_match:
                continue

            # data conversion
            yield ZTMRouteVariant(
                id=line_match[1],
                direction="0" if line_match[6] == "A" else "1",
                variant_order=line_match[7],
            )

        raise EOFError("End of section TR not reched before EOF!")

    def parse_lw(self) -> Iterator[ZTMVariantStop]:
        """
        Skips to next LW section and parses data from there.
        Yields ZTMVariantStop objects
        """
        # Create a set of possible regexps
        line_regexp = re.compile(r".*(\d{6})\s+[^,]{1,30}[\s,]+([\w-]{2})\s+\d\d\s+(NŻ|)\s*\|.*")
        zone_regexp = re.compile(r"=+\s+([\w\s]+)\s+=+")

        # Skip to wanted section
        self.skip_to_section("LW")

        # Default to zone 1
        zone: Literal["1", "1/2", "2", "2/O"] = "1"

        # Iterate over LW entries
        while (line := self.r.readline()):
            line = line.strip()

            # section end
            if "#LW" in line:
                return

            # regex for LW
            line_match = line_regexp.match(line)
            zone_match = zone_regexp.match(line) if line_match is None else None

            # change current zone
            if zone_match:
                zone_txt = zone_match[1].upper()
                if zone_txt == "PRZYSTANEK GRANICZNY":
                    zone = "1/2"

                elif zone_txt == "S T R E F A   1":
                    zone = "1"

                elif zone_txt == "S T R E F A   2":
                    zone = "2"

                else:
                    raise ValueError(f"Unrecognized zone description inside LW: {zone_txt!r}")

            elif line_match:
                stop_data = ZTMVariantStop(
                    id=line_match[1],
                    on_demand=(line_match[3] == "NŻ"),
                    zone=zone,
                )

                # Separate zone for Otwock is required, as L20 & L22 tickets
                # are cheaper when used only within Otwock municipality
                if zone == "2" and line_match[2] == "OT":
                    stop_data.zone = "2/O"

                yield stop_data

        raise EOFError("End of section LW not reched before EOF!")

    def _parse_single_wgod(self, route_type: str, route_id: str) -> Iterator[ZTMTTableDep]:
        """
        Yield data for every trip inside section *OD and combine data with
        info about accessibility included in section *WG
        """
        inside_wg = True
        inside_od = False

        # wg_regexp = re2.compile(r"G\s+\d*\s+(\d*):\s+(.+)")
        # od_regexp = re2.compile(r"([\d.]+)\s+(.+){1,17}")

        accessible_departures: Dict[str, bool] = {}

        while (line := self.r.readline()):
            line = line.strip()

            # section marks
            if line.startswith("#WG"):
                inside_wg = False
                continue

            elif line.startswith("*OD"):
                inside_od = True
                continue

            elif line.startswith("#OD"):
                return

            # line_split for parsing data
            line_split = line.split()

            # parse WG contents
            if inside_wg:

                if len(line_split) < 4:
                    continue

                # data conversion
                hour = line_split[2].rstrip(":")

                for dep in line_split[3:]:
                    minutes = _remove_non_digits(dep)

                    # Only trams can be un-accessible
                    if route_type == "0" and not dep.startswith("["):
                        accessible = False
                    else:
                        accessible = True

                    accessible_departures[normal_time(hour + "." + minutes)] = accessible

            # prase OD contents
            elif inside_od:
                if len(line_split) < 2:
                    continue

                # data conversion
                time = normal_time(line_split[0], lessthen24=True)
                trip_id = route_id + "/" + line_split[1]

                # ignore departures not found inside OD
                if time not in accessible_departures:
                    self.logger.warn(f"Departure in OD ({time}, {trip_id}) "
                                     "unmatched with anything in WG")
                    continue

                # combine data from OD with data from WG
                yield ZTMTTableDep(
                    trip_id=trip_id,
                    time=time,
                    accessible=accessible_departures[time],
                )

        raise EOFError("End of section WG/OD not reched before EOF!")

    def parse_wgod(self, route_type: str, route_id: str) -> Iterator[ZTMTTableDep]:
        """
        Skips to next WG section and parses data from (WG, OD) section pairs.
        Pairs are parsed until the end of section PR.
        Yields ZTMTTableDep objects.
        """
        while self.find_another("WG", "RP"):
            yield from self._parse_single_wgod(route_type, route_id)

    def parse_ll(self) -> Iterator[ZTMRoute]:
        """
        Skips to next LL section and parse it.
        Yields ZTMRoute objects.
        """
        # Python's regex is the same as re2
        regexp = re.compile(r"Linia:\s+([A-Za-z0-9-]{1,3})  - (.+)")
        self.skip_to_section("LL")

        while (line := self.r.readline()):
            line = line.strip()

            # section end
            if "#LL" in line:
                return

            # regex for TR
            line_match = regexp.match(line)

            if not line_match:
                continue

            yield ZTMRoute(id=line_match[1], desc=line_match[2])

        raise EOFError("End of section LL not reched before EOF!")

    def skip_to_section(self, section_code: str, end: bool = False):
        """
        Skips to provided section, so that the next line will be the one after *SECTION_CODE,
        or #SECTION_CODE if end is True.
        """
        join_char = "*" if not end else "#"
        search_for = join_char + section_code

        while (line := self.r.readline()):
            line = line.strip()

            if line.startswith(search_for):
                return

        raise EOFError(f"{search_for} not found before EOF")

    def find_another(self, section: str, finish: str) -> bool:
        """
        Check if there is another section, until finish-section is reached.
        Returns True if a section start was encountered,
        and False if closing tag of finish-section was reached.
        """
        finish = "#" + finish
        section = "*" + section

        while (line := self.r.readline()):
            line = line.strip()

            if line.startswith(finish):
                return False

            elif line.startswith(section):
                return True

        raise EOFError(f"Start of section {section} or end of {finish} not found before EOF")
