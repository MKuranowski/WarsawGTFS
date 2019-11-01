import re

from warnings import warn
from .utils_static import normal_time

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
class Parser:
    def __init__(self, reader):
        self.reader = reader

    def __del__(self):
        try:
            self.reader.close()
        except:
            pass

    def close(self):
        self.reader.close()

    def parse_ka(self):
        """
        Skips to section KA and parses data from there.
        A generator which yields for every day a:
        {
            "date": "YYYYMMDD",
            "services": ["service_id", ...],
        }
        """

        self.skip_to_section("KA")

        for line in self.reader:
            line = line.strip()

            # section end
            if "#KA" in line:
                return

            # regex for KA
            line_match = re.match(r"(\d{4})-(\d{2})-(\d{2})\s+\d\s+([\w\s]+)", line)

            if not line_match:
                continue

            # data conversion
            day_data = {
                "date": line_match[1] + line_match[2] + line_match[3],
                "services": line_match[4].split(),
            }

            yield day_data

        raise EOFError("End of section KA not reched before EOF!")

    def parse_zp(self):
        """
        Skips to section ZP and parses data from there.
        A generator which yields for every stop group a:
        {
            "id": "stop group id (4 digits)",
            "name": "stop_group name",
            "town": "stop_group town",
            "town_code": "stop_group town two letter code"
        }
        """
        self.skip_to_section("ZP")

        for line in self.reader:
            line = line.strip()

            # section end
            if "#ZP" in line:
                return

            # regex for ZP
            line_match = re.match(r"(\d{4})\s+([^,]{,30})[\s,]+([\w-]{2})\s+(.*)", line)

            if not line_match:
                continue

            # combine data
            group_data = {
                "id": line_match[1],
                "name": line_match[2],
                "town_code": line_match[3],
                "town": line_match[4].title(),
            }

            yield group_data

        raise EOFError("End of ZP section was not reached before EOF!")

    def parse_pr(self):
        """
        Skips to next PR section and parses data from there.
        A generator which yields for every stop inside group a:
        {
            "wheelchair": "0"/"1"/"2" (as in GTFS),
            "lat": float()/None,
            "lon": float()/None,
            "id": "stop id",
            "code": "stake id within group"
        }
        """
        self.skip_to_section("PR")

        for line in self.reader:
            line = line.strip()

            # Section end
            if "#PR" in line:
                return

            # regex for matching data of a stake inside a group
            line_match = re.match(r"(\d{4})(\d{2}).+Y=\s?([0-9Yy.]+)\s+X=\s?([0-9Xx.]+)\s+Pu=([0-9?])", line)

            if not line_match:
                continue

            # parse data
            stop_data = {}

            # convert accessibility info → GTFS
            if line_match[5] == "?": stop_data["wheelchair"] = "0"
            elif int(line_match[5]) > 5: stop_data["wheelchair"] = "2"
            else: stop_data["wheelchair"] = "1"

            # convert stop poisition
            if "y" in line_match[3].lower() or "x" in line_match[4].lower():
                stop_data["lat"] = None
                stop_data["lon"] = None

            else:
                stop_data["lat"] = float(line_match[3])
                stop_data["lon"] = float(line_match[4])

            stop_data["id"] = line_match[1] + line_match[2]
            stop_data["code"] = line_match[2]

            yield stop_data

        raise EOFError("End of PR section was not reached before EOF!")

    def parse_wk(self, route_id):
        """
        Skips to next WK section and parses data from there.
        A generator which yields for every trip:
        {
            "id": "trip id",
            "stops": [
                {"stop": "stop id",
                 "time": "HH:MM:SS",
                 "flags": ""/"P"/"B"
                 }, ... ]
        }
        """
        self.skip_to_section("WK")

        trip_data = {"id": "", "stops": []}

        for line in self.reader:
            line = line.strip()

            # section end
            if "#WK" in line:

                # yield last trip
                if trip_data["stops"]:
                    yield trip_data

                return

            # regex for wk
            line_match = re.match(r"(\S{,17})\s+(\d{6})\s+\w{2}\s+([0-9.]+)(\s+\w|)", line)

            if not line_match:
                continue

            # data conversion
            stoptime_data = {
                "stop": line_match[2],
                "time": normal_time(line_match[3]),
                "flags": line_match[4].strip(),
            }

            trip_id = route_id + "/" + line_match[1]

            # no active trip
            if not trip_data["id"]:
                trip_data = {"id": trip_id, "stops": []}

            # change of active trip
            elif trip_id != trip_data["id"]:
                yield trip_data

                trip_data = {"id": trip_id, "stops": []}

            # append stop to active trip
            trip_data["stops"].append(stoptime_data)

        raise EOFError("End of section WK not reched before EOF!")

    def parse_tr(self):
        """
        Skips to next TR section and parses data from there.
        A generator which yields for every variant of route:
        {
            "direction": "0"/"1",
            "variant_order": "0"/"1"/...,
        }
        """
        self.skip_to_section("TR")

        for line in self.reader:
            line = line.strip()

            # section end
            if "#TR" in line:
                return

            # regex for TR
            line_match = re.match(r"([\w-]+)\s*,\s+([^,]{,30})[\s,]+([\w-]{2})\s+==>\s+([^,]{,30})[\s,]+([\w-]{2})\s+Kier\. (\w)\s+Poz. (\w)", line)

            if not line_match:
                continue

            # data conversion
            variant_data = {
                "id": line_match[1],
                "direction": "0" if line_match[6] == "A" else "1",
                "variant_order": line_match[7],
            }

            yield variant_data

        raise EOFError("End of section TR not reched before EOF!")

    def parse_lw(self):
        """
        Skips to next LW section and parses data from there.
        A generator which yields for every stop of variant:
        {
            "id": "stop id",
            "on_demand": bool(),
            "zone": "1" / "1/2" / "2" / "2/O",
        }
        """
        self.skip_to_section("LW")

        zone = "1"

        for line in self.reader:
            line = line.strip()

            # section end
            if "#LW" in line:
                return

            # regex for LW
            line_match = re.match(r".*(\d{6})\s+[^,]{,30}[\s,]+([\w-]{2})\s+\d\d\s+(NŻ|)\s*\|.*", line)
            zone_match = re.match(r"=+\s+([\w\s]+)\s+=+", line)

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
                    raise ValueError("Unrecognized zone description inside LW: {!r}".format(zone_txt))


            elif line_match:
                stop_data = {
                    "id": line_match[1],
                    "on_demand": (line_match[3] == "NŻ"),
                }

                # Separate zone for Otwock is required, as L20 & L22 tickets
                # are cheaper when used only within Otwock municipality
                if zone == "2" and line_match[2] == "OT":
                    stop_data["zone"] = "2/O"
                else:
                    stop_data["zone"] = zone

                yield stop_data

        raise EOFError("End of section LW not reched before EOF!")

    def _parse_single_wgod(self, route_type, route_id):
        inside_wg = True
        inside_od = False

        wg_data = {}

        for line in self.reader:
            line = line.strip()

            # section end
            if "#WG" in line:
                inside_wg = False
                continue

            elif "*OD" in line:
                inside_od = True
                continue

            elif "#OD" in line:
                inside_od = False
                return

            if inside_wg:

                # regex for WG
                line_match = re.match(r"G\s+\d*\s+(\d*):\s+(.+)", line)

                if not line_match:
                    continue

                # data conversion
                hour = line_match[1]

                for dep in line_match[2].split():
                    minutes = re.sub(r"\D", "", dep)

                    # All Busses & Trains are wheelchair accessible - don't bother checking
                    if route_type == "0" and not dep.startswith("["):
                        accessible = False
                    else:
                        accessible = True

                    wg_data[ normal_time(hour + "." + minutes) ] = {
                        "accessible": accessible,
                    }

            elif inside_od:
                # regex for OD
                line_match = re.match(r"([\d.]+)\s+(.+){,17}", line)

                if not line_match:
                    continue

                # data conversion
                time = normal_time(line_match[1], lessthen24=True)
                trip_id = route_id + "/" + line_match[2]

                # ignore departures not found inside OD
                if time not in wg_data:
                    warn("Departure in OD ({}, {}) unmatched with anything in WG".format(time, trip_id))
                    continue

                # combine data from OD with data from WG
                dep_data = {
                    **wg_data[time],
                    "time": time,
                    "id": trip_id,
                }

                yield dep_data

        raise EOFError("End of section WG/OD not reched before EOF!")

    def parse_wgod(self, route_type, route_id):
        """
        Skips to next WG section and join data from WG to OD section,
        and parse it until finiding the end of section PR.
        A generator which yields for every departure for every timetable:
        {
            "time": "HH:MM:SS",
            "trip_id": trip_id,
            "accessible": bool(),
        }
        """
        more_wgod = self.find_another("WG", "RP")

        while more_wgod == "more":
            yield from self._parse_single_wgod(route_type, route_id)

            more_wgod = self.find_another("WG", "RP")

    def parse_ll(self):
        """
        Skips to next LL section and parse it.
        A generator which yields for every route:
        {
            "id": "route id",
            "desc": "route type description",
        }
        """
        self.skip_to_section("LL")

        for line in self.reader:
            line = line.strip()

            # section end
            if "#LL" in line:
                return

            # regex for TR
            line_match = re.match(r"Linia:\s+([A-Za-z0-9-]{1,3})  - (.+)", line)

            if not line_match:
                continue

            # data conversion
            route_data = {
                "id": line_match[1],
                "desc": line_match[2],
            }

            yield route_data

        raise EOFError("End of section LL not reched before EOF!")

    def skip_to_section(self, section, end=False):
        join_char = "*" if not end else "#"
        for line in self.reader:
            line = line.strip()

            if (join_char + section) in line:
                return

        raise EOFError("Start of section " + section + " not found before EOF")

    def find_another(self, section, finish):
        for line in self.reader:
            line = line.strip()

            if ("#" + finish) in line:
                return "end"

            elif ("*" + section) in line:
                return "more"

        raise EOFError("Start of section " + section + " or " + finish + " not found before EOF")
