from google.transit import gtfs_realtime_pb2 as gtfs_rt
from collections import OrderedDict
from datetime import datetime, timedelta
from tempfile import TemporaryFile
from bs4 import BeautifulSoup
from urllib import request
from copy import copy
import feedparser
import requests
import sqlite3
import zipfile
import math
import json
import csv
import re
import os
import io

ALERT_FLAGS = {"autobusy", "tramwaje", "skm", "kolej", "metro"}

# Some random Functions

def _DictFactory(cursor, row):
    "A simple DictFactory that returns data in a dict"
    d = {}
    for idx, col in enumerate(cursor.description): d[col[0]] = row[idx]
    return d

def _ListRoutes():
    "Give a list of active, routes. Returns a dict {ROUTE_TYPE: [ROUTE_1, ROUTE_2]}"
    routes = {0: [], 1: ["M1", "M2"], 2: [], 3: []}

    website = requests.get("http://m.ztm.waw.pl/rozklad_nowy.php?c=182&l=1")
    website.raise_for_status()
    website.encoding = "utf-8"

    soup = BeautifulSoup(website.text, "html.parser")

    for div in soup.find_all("div", class_="LineList"):
        for link in div.find_all("a"):
            route_num = link.get_text().strip()
            route_desc = link.get("title", "").lower()

            # Trams
            if "tram" in route_desc: routes[0].append(route_num)

            # Ignore KM and WKD routes, add only SKM
            elif "mazowieckich" in route_desc: continue
            elif "dojazdowej" in route_desc: continue
            elif "miejskiej" in route_desc: routes[2].append(route_num)

            # Buses
            else: routes[3].append(route_num)

    return routes

def _FilterLines(rlist):
    "Filter lines in ZTM alerts to match ids in GTFS"
    for x in copy(rlist):
        if x in ["", "Z", "WKD", "POP", "INFO", "WLT"]:
            while x in rlist: rlist.remove(x)

        elif x.startswith("M") and x not in ["M1", "M2"]:
            while x in rlist: rlist.remove(x)
            if "M1" not in rlist: rlist.add("M1")
            if "M2" not in rlist: rlist.add("M2")

        elif x.startswith("S") and x not in ["S1", "S2", "S3", "S9"]:
            while x in rlist: rlist.remove(x)
            if "S1" not in rlist: rlist.add("S1")
            if "S2" not in rlist: rlist.add("S2")
            if "S3" not in rlist: rlist.add("S3")
            if "S9" not in rlist: rlist.add("S9")

        elif x.startswith("KM") or x.startswith("R") or (x.startswith("9") and len(x) == 3):
            while x in rlist: rlist.remove(x)

    return rlist

def _CleanTags(html):
    "Clean text from html tags"
    if html == "None": return ""
    else: return re.sub("<.*?>", "", html)

def _AlertFlags(descsoup):
    "Get additional flags about the alert from icons, passed as BS4's soup"
    flags = set()
    for icon in descsoup.find_all("td", class_="ico"):
        flags |= {i.get("title") for i in icon.find_all("img")}
    return flags.intersection(ALERT_FLAGS)

def _AlertDesc(descsoup):
    "Get alert description from BS4's soup"
    # Remove unnecessary text
    for tag in descsoup.find_all("table"): tag.decompose()
    for tag in descsoup.find_all("h4"): tag.decompose()
    for tag in descsoup.find_all("div", id="PageInfo"): tag.decompose()
    for tag in descsoup.find_all("div", id="InneKomunikaty"): tag.decompose()
    for tag in descsoup.find_all("div", class_="InneKomunikatyLinia"): tag.decompose()
    for tag in descsoup.find_all("div", class_="cb"): tag.decompose()

    # Get what's left overr
    desc_with_tags = str(descsoup)

    # Clean text from HTML tags
    clean_desc = _CleanTags(desc_with_tags.replace("</p>", "\n").replace("<br/>", "\n").replace("<br>", "\n").replace("\xa0", " ").replace("  "," "))

    return clean_desc, desc_with_tags

def _FindTrip(timepoint, route, stop, times):
    "Try find trip_id in times for given timepoint route and stop"
    times = list(filter(lambda x: x["routeId"] == route and x["stopId"] == stop, times))
    trips = list(filter(lambda x: x["timepoint"] == timepoint, times))
    if trips: return(trips[0]["tripId"])
    # If not found, try to add 24h to timepoint, to catch after midnight trips
    timepointAM = ":".join([str(int(timepoint.split(":")[0]) + 24), timepoint.split(":")[1], timepoint.split(":")[2]])
    trips = list(filter(lambda x: x["timepoint"] == timepointAM, times))
    if trips: return(trips[0]["tripId"])
    #else:
        #print("Trip not found for R%s S%s T%s" % (route, stop, timepoint))

def _TimeDifference(t1, t2):
    "Check if t2 happended after t1"
    t1 = [int(x) for x in t1.split(":")]
    t2 = [int(x) for x in t2.split(":")]
    if t2[0] >= 24 and t1[0] <= 3: t1[0] += 24 # Fix for after-midnight trips
    if t1[0] < t2[0]: return(True)
    elif t1[0] == t2[0] and t1[1] < t2[1]: return(True)
    elif t1[0] == t2[0] and t1[1] == t2[1] and t1[2] <= t2[2]: return(True)
    else: return False

def _Distance(pos1, pos2):
    "Calculate the distance between pos1 and pos2 in kilometers"
    lat1, lon1, lat2, lon2 = map(math.radians, [pos1[0], pos1[1], pos2[0], pos2[1]])
    lat, lon = lat2 - lat1, lon2 - lon1
    dist = 2 * 6371 * math.asin(math.sqrt(math.sin(lat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(lon * 0.5) ** 2))
    return dist

def _Bearing(pos1, pos2):
    "Calculate initial bearing of vehicle, only if the vehicle has moved more than 30m"
    if _Distance(pos1, pos2) < 0.003: return None
    lat1, lat2, lon = map(math.radians, [pos1[0], pos2[0], pos2[1] - pos1[1]])
    x = math.sin(lon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(lon))
    return math.degrees(math.atan2(x, y))

def _SimplifyResponse(api_response):
    result = []
    for item in api_response["result"]:
        item_dict = {}
        for kv_pair in item["values"]:
            if "key" not in kv_pair or "value" not in kv_pair: continue
            if kv_pair["value"] == "null": kv_pair["value"] = None
            item_dict[kv_pair["key"]] = kv_pair["value"]
        result.append(item_dict)

    return result

# Main Functions

def Alerts(out_proto=True, out_json=False):
    "Get ZTM Warszawa Alerts"
    # Grab Entries
    changes = feedparser.parse("http://www.ztm.waw.pl/rss.php?l=1&IDRss=3").entries
    disruptions = feedparser.parse("http://www.ztm.waw.pl/rss.php?l=1&IDRss=6").entries
    gtfs_routes = _ListRoutes()
    idenum = 0

    # Containers
    if out_proto:
        container = gtfs_rt.FeedMessage()
        header = container.header
        header.gtfs_realtime_version = "2.0"
        header.incrementality = 0
        header.timestamp = round(datetime.today().timestamp())

    if out_json:
        json_container = {"time": datetime.today().strftime("%Y-%m-%d %H:%M:%S"), "alerts": []}

    # Sort Entries
    all_entries = []
    for i in disruptions:
        i.effect = 2 # Reduced Service
        all_entries.append(i)

    for i in changes:
        i.effect = 7 # Other Effect
        all_entries.append(i)

    # Alerts
    for entry in all_entries:
        idenum += 1
        try: lines_raw = entry.title.split(":")[1].strip()
        except IndexError: lines_raw = ""
        lines = _FilterLines(set(re.findall(r"[0-9a-zA-Z-]{1,3}", lines_raw)))

        # Gather data
        alert_id = "-".join(["a", str(idenum)])
        link = _CleanTags(str(entry.link))
        title = _CleanTags(str(entry.description))

        try:
            # Additional info from website provided by RSS
            alert_website = requests.get(link)
            alert_website.raise_for_status()
            alert_website.encoding = "utf-8"

            soup = BeautifulSoup(alert_website.text, "html.parser")
            descsoup = soup.find("div", id="PageContent")

            # Add routes if those are not specified
            if not lines:
                flags = _AlertFlags(descsoup)

                if "metro" in flags: lines |= set(gtfs_routes[1])
                elif "tramwaje" in flags: lines |= set(gtfs_routes[0])
                elif flags.intersection("kolej", "skm"): lines |= set(gtfs_routes[2])
                elif "autobusy" in flags: lines |= set(gtfs_routes[3])

            desc, desc_html = _AlertDesc(descsoup)

        except:
            desc, desc_html = "", ""

        if lines:

            # Append to gtfs_rt container
            if out_proto:
                entity = container.entity.add()
                entity.id = alert_id
                alert = entity.alert
                alert.effect = entry.effect
                alert.url.translation.add().text = link
                alert.header_text.translation.add().text = title
                if desc: alert.description_text.translation.add().text = desc
                for line in sorted(lines):
                    selector = alert.informed_entity.add()
                    selector.route_id = line

            # Append to JSON container
            if out_json:
                json_container["alerts"].append(OrderedDict((
                    ("id", alert_id), ("routes", sorted(lines)),
                    ("effect", "REDUCED_SERVICE" if entry.effect == 2 else "OTHER_EFFECT"),
                    ("link", link), ("title", title), ("body", desc), ("htmlbody", desc_html)
                )))

    # Export
    if out_proto:
        with open("output-rt/alerts.pb", "w") as f: f.write(str(container))
        with open("output-rt/alerts.pbn", "wb") as f: f.write(container.SerializeToString())

    if out_json:
        with open("output-rt/alerts.json", "w", encoding="utf8") as f: json.dump(json_container, f, indent=2)

def Brigades(apikey, gtfsloc="https://mkuran.pl/feed/ztm/ztm-latest.zip", export=False):
    "Create a brigades table to match positions to gtfs"
    # Variables
    active_services = set()
    active_routes = set()
    stop_positions = {}
    brigades = {}

    trip_last_points = {}
    api_responses = {}
    parsed_stops = set()
    matched_trips = set()

    today = datetime.today().strftime("%Y%m%d")

    # Download GTFS
    if gtfsloc.startswith("https://") or gtfsloc.startswith("ftp://") or gtfsloc.startswith("http://"):
        gtfs_request = requests.get(gtfsloc)
        gtfs_file = TemporaryFile()
        gtfs_file.write(gtfs_request.content)
        gtfs_file.seek(0)

    else:
        gtfs_file = open(gtfsloc, mode="rb")

    # Read GTFS
    print("Reading routes, services and stops from GTFS")
    gtfs_zip = zipfile.ZipFile(gtfs_file)

    # Routes suitable for matching brigades
    with gtfs_zip.open("routes.txt") as routes:
        reader = csv.DictReader(io.TextIOWrapper(routes, encoding="utf-8", newline=""))
        for line in reader:
            if line["route_type"] in ["0", "3"]:
                brigades[line["route_id"]] = {}
                active_routes.add(line["route_id"])

    # Service_ids active today
    with gtfs_zip.open("calendar_dates.txt") as calendars:
        reader = csv.DictReader(io.TextIOWrapper(calendars, encoding="utf-8", newline=""))
        for line in reader:
            if line["date"] == today:
                active_services.add(line["service_id"])

    # Stops for additional information used in parsing vehicles locations
    with gtfs_zip.open("stops.txt") as stops:
        reader = csv.DictReader(io.TextIOWrapper(stops, encoding="utf-8", newline=""))
        for line in reader:
            stop_positions[line["stop_id"]] = [line["stop_lat"], line["stop_lon"]]

    print("Matching stop_times.txt to brigades", end="\n\n")
    # And now open stop_times and match trip_id with brigade, by matching route_id+stop_id+departure_time
    with gtfs_zip.open("stop_times.txt") as stoptimes:
        reader = csv.DictReader(io.TextIOWrapper(stoptimes, encoding="utf-8", newline=""))
        for line in reader:
            trip_id = line["trip_id"]
            route_id = trip_id.split("/")[0]

            try: service_id = trip_id.split("/")[2]
            except IndexError: continue

            # Ignore nonactive routes&services
            if route_id not in active_routes or service_id not in active_services:
                continue

            stop_id = line["stop_id"]
            stop_index = int(line["stop_sequence"])
            timepoint = line["departure_time"]

            print("\033[1A\033[KNext stop_time row: T:", trip_id, "I:", stop_index, "({})".format(timepoint))

            # If considered timepoint of a trip happens »later« then what's stored in trip_last_points
            # Then write current stoptime info as »last_stop of a trip«
            if trip_last_points.get(trip_id, {}).get("index", -1) < stop_index:
                trip_last_points[trip_id] = {"stop": stop_id, "index": stop_index, "timepoint": timepoint}


            # If there's no brigade for this trip, try to match it
            if trip_id not in matched_trips:
                if (route_id, stop_id) not in api_responses:

                    try:
                        print("\033[1A\033[KMaking new API call: R:", route_id, "S:", stop_id)
                        api_response = requests.get(
                            "https://api.um.warszawa.pl/api/action/dbtimetable_get/",
                            timeout = 5,
                            params = {
                                "id": "e923fa0e-d96c-43f9-ae6e-60518c9f3238",
                                "apikey": apikey,
                                "busstopId": stop_id[:4],
                                "busstopNr": stop_id[4:6],
                                "line": route_id
                        })
                        api_response.raise_for_status()

                        print("\033[1A\033[KReading recived API response for: R:", route_id, "S:", stop_id)

                        api_response = api_response.json()
                        assert api_response["result"] != "false"
                        result = _SimplifyResponse(api_response)

                    except (json.decoder.JSONDecodeError,
                            requests.exceptions.HTTPError,
                            requests.exceptions.ConnectTimeout,
                            requests.exceptions.ReadTimeout,
                            AssertionError
                    ):
                        print("\033[1A\033[K\033[1mIncorrent API response for R: {} S: {}\033[0m".format(route_id, stop_id), end="\n\n")
                        continue

                    api_responses[(route_id, stop_id)] = result

                else:
                    result = api_responses[(route_id, stop_id)]

                    for departure in result:
                        if departure.get("czas") == timepoint:
                            brigade_id = departure.get("brygada", "").lstrip("0")
                            break
                    else:
                        brigade_id = ""

                    if not brigade_id: continue

                    matched_trips.add(trip_id)
                    if brigade_id not in brigades[route_id]: brigades[route_id][brigade_id] = []
                    brigades[route_id][brigade_id].append({"trip_id": trip_id})

    gtfs_zip.close()
    gtfs_file.close()

    print("\033[1A\033[KMatching stop_times.txt to brigades: done")

    # Sort everything
    print("Appending info about last timepoint to brigade")
    for route in brigades:
        for brigade in brigades[route]:
            brigades[route][brigade] = sorted(brigades[route][brigade], key=lambda i: i["trip_id"].split("/")[-1])

            for trip in brigades[route][brigade]:
                trip_last_point = trip_last_points[trip["trip_id"]]
                trip["last_stop_id"] = trip_last_point["stop"]
                trip["last_stop_latlon"] = stop_positions[trip_last_point["stop"]]
                trip["last_stop_timepoint"] = trip_last_point["timepoint"]

        brigades[route] = OrderedDict(sorted(brigades[route].items()))

    if export:
        print("Exporting")
        with open("output-rt/brigades.json", "w") as jsonfile:
            jsonfile.write(json.dumps(brigades, indent=2))
    return brigades

def Positions(apikey, brigades="https://mkuran.pl/feed/ztm/ztm-brigades.json", previous={}, out_proto=True, out_json=False):
    "Get ZTM Warszawa positions"
    # Variables
    positions = OrderedDict()
    source = []

    # GTFS-RT Container
    if out_proto:
        container = gtfs_rt.FeedMessage()
        header = container.header
        header.gtfs_realtime_version = "2.0"
        header.incrementality = 0
        header.timestamp = round(datetime.today().timestamp())

    # JSON Container
    if out_json:
        json_container = OrderedDict()
        json_container["time"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        json_container["positions"] = []

    # Get brigades, if brigades is not already a dict or OrderedDict
    if type(brigades) is str:
        if brigades.startswith("ftp://") or brigades.startswith("http://") or brigades.startswith("https://"):
            brigades = request.urlopen(brigades).read()
            brigades = json.loads(brigades)
        else:
            with open(brigades) as f:
                brigades = json.loads(f.read())

    # Sort times in brigades, if they're not sorted
    if type(brigades) is not OrderedDict:
        for route in brigades:
            for brigade in brigades[route]:
                brigades[route][brigade] = sorted(brigades[route][brigade], key= \
                    lambda x: x["trip_id"].split("/")[-1])

    # Load data from API UM
    sourceBuses = str(request.urlopen("https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=%20f2e5503e-%20927d-4ad3-9500-4ab9e55deb59&apikey={}&type=1".format(apikey)).read(), "utf-8")
    sourceBuses = json.loads(sourceBuses)
    sourceTrams = str(request.urlopen("https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=%20f2e5503e-%20927d-4ad3-9500-4ab9e55deb59&apikey={}&type=2".format(apikey)).read(), "utf-8")
    sourceTrams = json.loads(sourceTrams)

    # Check if response from API UM is correct, and add it to positions list
    if type(sourceTrams["result"]) is list: source += sourceTrams["result"]
    else: print("WarsawGTFS-RT: Incorrect trams positions response")
    if type(sourceBuses["result"]) is list: source += sourceBuses["result"]
    else: print("WarsawGTFS-RT: Incorrect buses positions response")
    del sourceBuses, sourceTrams

    # Iterate over results
    for v in source:
        # Read data about position
        lat, lon, route, brigade = v["Lat"], v["Lon"], v["Lines"], v["Brigade"].lstrip("0")
        tstamp = datetime.strptime(v["Time"], "%Y-%m-%d %H:%M:%S")
        trip_id = ""
        bearing = None
        id = "-".join(["v", route, brigade])
        triplist = brigades.get(route, {}).get(brigade, [])
        if not triplist: continue

        # Do not care about obsolete data
        if (datetime.today() - tstamp) > timedelta(minutes=10): continue

        # Try to match with trip
        if id in previous:
            prev_trip, prev_lat, prev_lon, prev_bearing = previous[id]["trip_id"], previous[id]["lat"], previous[id]["lon"], previous[id].get("bearing", None)
            tripidslist = [x["trip_id"] for x in triplist]

            # Get vehicle bearing
            bearing = _Bearing([prev_lat, prev_lon], [lat, lon])
            if (not bearing) and prev_bearing: bearing = prev_bearing

            # If vehicle was doing its last trip, there's nothing more that can be calculated
            if prev_trip == triplist[-1]["trip_id"]:
                trip_id = copy(prev_trip)

            # The calculations require for the prev_trip to be in the triplist
            elif prev_trip in tripidslist:
                prev_trip_index = tripidslist.index(prev_trip)
                prev_trip_last_latlon = list(map(float, triplist[prev_trip_index]["last_stop_latlon"]))
                # If vehicle is near (50m) the last stop => the trip has finished => assume the next trip
                # Or if the previous trip should've finished 30min earlier (A fallback rule if the previous cause has failed)
                if _Distance([lat, lon], prev_trip_last_latlon) <= 0.05 or \
                    _TimeDifference(triplist[prev_trip_index]["last_stop_timepoint"], (datetime.now()-timedelta(minutes=30)).strftime("%H:%M:%S")):
                    trip_id = triplist[prev_trip_index + 1]["trip_id"]
                else:
                    trip_id = copy(prev_trip)

        if not trip_id:
            # If the trip_id still is not defined, assume the trip is not delayed
            currtime = datetime.now().strftime("%H:%M:%S")
            for trip in triplist:
                if _TimeDifference(currtime, trip["last_stop_timepoint"]):
                    trip_id = trip["trip_id"]
                    break
            if not trip_id: trip_id = triplist[-1]["trip_id"] # If the trips still couldn't be found - assume it's doing the last trip

        # Save to dict
        data = OrderedDict()
        data["id"] = copy(id)
        data["trip_id"] = copy(trip_id)
        data["timestamp"] = copy(tstamp)
        data["lat"] = copy(lat)
        data["lon"] = copy(lon)
        if bearing: data["bearing"] = copy(bearing)
        positions[id] = copy(data)

        # Save to gtfs_rt container
        if out_proto:
            entity = container.entity.add()
            entity.id = id
            vehicle = entity.vehicle
            vehicle.trip.trip_id = trip_id
            vehicle.vehicle.id = id
            vehicle.position.latitude = float(lat)
            vehicle.position.longitude = float(lon)
            if bearing: vehicle.position.bearing = float(bearing)
            vehicle.timestamp = round(tstamp.timestamp())

    # Export results
    if out_proto:
        with open("output-rt/vehicles.pb", "w") as f: f.write(str(container))
        with open("output-rt/vehicles.pbn", "wb") as f: f.write(container.SerializeToString())

    if out_json:
        for i in map(copy, positions.values()):
            i["timestamp"] = i["timestamp"].isoformat()
            json_container["positions"].append(i)
        with open("output-rt/vehicles.json", "w", encoding="utf8") as f: json.dump(json_container, f, indent=2)

    return positions

# A simple interface
if __name__ == "__main__":
    import argparse
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-a", "--alerts", action="store_true", required=False, dest="alerts", help="parse alerts into output-rt/")
    argprs.add_argument("-b", "--brigades", action="store_true", required=False, dest="brigades", help="parse brigades into output-rt/")
    argprs.add_argument("-p", "--positions", action="store_true", required=False, dest="positions", help="parse positions into output-rt/")
    argprs.add_argument("-k", "--key", default="", required=False, metavar="(apikey)", dest="key", help="apikey from api.um.warszawa.pl")

    argprs.add_argument("--gtfs-file", default="https://mkuran.pl/feed/ztm/ztm-latest.zip", required=False, dest="gtfs_path", help="path/URL to the GTFS file")
    argprs.add_argument("--brigades-file", default="https://mkuran.pl/feed/ztm/ztm-brigades.json", required=False, dest="brigades_path", help="path/URL to brigades JSON file (created by option -b)")

    argprs.add_argument("--json", action="store_true", default=False, required=False, dest="json", help="output additionally rt data to .json format")
    argprs.add_argument("--no_protobuf", action="store_false", default=True, required=False, dest="proto", help="do not output rt data to GTFS-Realtime format")

    args = argprs.parse_args()

    if (args.brigades or args.positions) and (not args.key):
        raise ValueError("Apikey is required for brigades/positions")

    if not (args.json or args.proto):
        raise ValueError("No output filetype specified")

    if args.alerts:
        print("Parsing Alerts")
        Alerts(out_proto=args.proto, out_json=args.json)

    if args.brigades and args.key:
        print("Parsing brigades")
        Brigades(apikey=args.key, gtfsloc=args.gtfs_path, export=True)

    if args.positions and args.key:
        print("Parsing positions")
        Positions(apikey=args.key, brigades=args.brigades_path, out_proto=args.proto, out_json=args.json)
