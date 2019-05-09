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

from .utils_realtime import *
from .utils import *


# Main Functions
class Realtime:

    @staticmethod
    def alerts(gtfs_location="https://mkuran.pl/feed/ztm/ztm-latest.zip", out_proto=True, binary_proto=True, out_json=False):
        "Get ZTM Warszawa Alerts"
        # Grab Entries
        changes = feedparser.parse("http://www.ztm.waw.pl/rss.php?l=1&IDRss=3").entries
        disruptions = feedparser.parse("http://www.ztm.waw.pl/rss.php?l=1&IDRss=6").entries
        gtfs_routes = WarsawGtfs.routes_only(gtfs_location)
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
            # Gather data
            link = no_html(str(entry.link))
            title = no_html(str(entry.description))

            # Alert ID is hidden in the alert link
            alert_id = "A/" + re.search(r"(?<=&i=)\d+", link)[0]

            try: lines_raw = re.findall(r"[A-Za-z0-9-]{1,3}", entry.title.split(":")[1])
            except IndexError: lines_raw = ""

            lines = [i for i in lines_raw  if \
                i in gtfs_routes["0"] or \
                i in gtfs_routes["1"] or \
                i in gtfs_routes["2"] or \
                i in gtfs_routes["3"]
            ]


            #try:
            # Additional info from website provided by RSS
            alert_website = requests.get(link)
            alert_website.raise_for_status()
            alert_website.encoding = "utf-8"

            soup = BeautifulSoup(alert_website.text, "html.parser")
            descsoup = soup.find("div", id="PageContent")

            # Add routes if those are not specified
            if not lines:
                flags = alert_flags(descsoup)

                if "metro" in flags: lines.extend(gtfs_routes["1"])
                elif "tramwaje" in flags: lines.extend(gtfs_routes["0"])
                elif flags.intersection("kolej", "skm"): lines.extend(gtfs_routes["2"])
                elif "autobusy" in flags: lines.extend(gtfs_routes["3"])

            desc, desc_html = alert_description(descsoup)

            #except:
            #    desc, desc_html = "", ""

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
                    json_container["alerts"].append(OrderedDict([
                        ("id", alert_id), ("routes", sorted(lines)),
                        ("effect", "REDUCED_SERVICE" if entry.effect == 2 else "OTHER_EFFECT"),
                        ("link", link), ("title", title), ("body", desc), ("htmlbody", desc_html)
                    ]))

        # Export
        if out_proto and binary_proto:
            with open("gtfs-rt/alerts.pb", "wb") as f: f.write(container.SerializeToString())

        elif out_proto:
            with open("gtfs-rt/alerts.pb", "w") as f: f.write(str(container))

        if out_json:
            with open("gtfs-rt/alerts.json", "w", encoding="utf8") as f: json.dump(json_container, f, indent=2, ensure_ascii=False)

    @staticmethod
    def brigades(apikey, gtfs_location="https://mkuran.pl/feed/ztm/ztm-latest.zip", export=False):
        "Create a brigades table to match positions to gtfs"
        # Variables
        brigades = {}

        trip_last_points = {}
        api_responses = {}
        parsed_stops = set()
        matched_trips = set()

        today = datetime.today().strftime("%Y%m%d")

        # Download GTFS
        print("Retreaving GTFS")
        gtfs = WarsawGtfs(gtfs_location)

        print("Reading routes, services and stops from GTFS")
        gtfs.list()

        # We need only route_ids of trams and buses — other are not needed for brigades
        gtfs.routes = gtfs.routes["0"] | gtfs.routes["3"]

        print("Matching stop_times.txt to brigades", end="\n\n")
        # And now open stop_times and match trip_id with brigade,
        # by matching route_id+stop_id+departure_time with api.um.warszawa.pl schedules, which have brigade number
        with gtfs.arch.open("stop_times.txt", mode="r") as stoptimes:
            reader = csv.DictReader(io.TextIOWrapper(stoptimes, encoding="utf8", newline=""))

            for row in reader:

                trip_id = row["trip_id"]
                trip_id_split = trip_id.split("/")

                # e.g RA190507/1/TD-3BAN/DP/04.01_ from merged GTFS
                if len(trip_id_split) == 5:
                    route_id = trip_id_split[1]
                    service_id = trip_id_split[0] + "/" + trip_id_split[3]

                # e.g 1/TD-3BAN/DP/04.01_ from normal GTFS
                elif len(trip_id_split) == 4:
                    route_id = trip_id_split[0]
                    service_id = trip_id_split[2]

                # Unrecognized format (probably from metro) - ignore
                else:
                    continue

                # Ignore nonactive routes & services
                if route_id not in gtfs.routes or service_id not in gtfs.services:
                    continue

                # Other info about stop_time
                stop_id = row["stop_id"]
                stop_index = int(row["stop_sequence"])
                timepoint = row["departure_time"]

                print("\033[1A\033[K" + "Next stop_time row: T:", trip_id, "I:", stop_index, "({})".format(timepoint))

                # If considered timepoint of a trip happens »later« then what's stored in trip_last_points
                # Then write current stoptime info as »last_stop of a trip«
                if trip_last_points.get(trip_id, {}).get("index", -1) < stop_index:
                    trip_last_points[trip_id] = {"stop": stop_id, "index": stop_index, "timepoint": timepoint}

                # If there's no brigade for this trip, try to match it
                if trip_id not in matched_trips:
                    if (route_id, stop_id) not in api_responses:

                        try:
                            print("\033[1A\033[K" + "Making new API call: R:", route_id, "S:", stop_id)
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

                            print("\033[1A\033[K" + "Reading recived API response for: R:", route_id, "S:", stop_id)

                            api_response = api_response.json()
                            assert type(api_response["result"]) is list
                            result = parse_apium_response(api_response)

                        except (json.decoder.JSONDecodeError,
                                requests.exceptions.HTTPError,
                                requests.exceptions.ConnectTimeout,
                                requests.exceptions.ReadTimeout,
                                AssertionError
                        ):
                            print("\033[1A\033[K\033[1m" + "Incorrent API response for R: {} S: {}\033[0m".format(route_id, stop_id), end="\n\n")
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

                    if route_id not in brigades: brigades[route_id] = {}
                    if brigade_id not in brigades[route_id]: brigades[route_id][brigade_id] = []

                    brigades[route_id][brigade_id].append({"trip_id": trip_id})

        gtfs.close()

        print("\033[1A\033[K" + "Matching stop_times.txt to brigades: done")

        # Sort everything
        print("Appending info about last timepoint to brigade")
        for route in brigades:
            for brigade in brigades[route]:
                brigades[route][brigade] = sorted(brigades[route][brigade], key=lambda i: i["trip_id"].split("/")[-1])

                for trip in brigades[route][brigade]:
                    trip_last_point = trip_last_points[trip["trip_id"]]
                    trip["last_stop_id"] = trip_last_point["stop"]
                    trip["last_stop_latlon"] = gtfs.stops[trip_last_point["stop"]]
                    trip["last_stop_timepoint"] = trip_last_point["timepoint"]

            brigades[route] = OrderedDict(sorted(brigades[route].items()))

        if export:
            print("Exporting")
            with open("gtfs-rt/brigades.json", "w") as jsonfile:
                jsonfile.write(json.dumps(brigades, indent=2))

        return brigades

    @staticmethod
    def positions(apikey, brigades="https://mkuran.pl/feed/ztm/ztm-brigades.json", previous={}, out_proto=True, binary_proto=True, out_json=False):
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
                brigades_request = requests.get(brigades)
                brigades = brigades_request.json()
            else:
                with open(brigades) as f:
                    brigades = json.load(f)

        # Sort times in brigades, if they're not sorted
        if type(brigades) is not OrderedDict:
            for route in brigades:
                for brigade in brigades[route]:
                    brigades[route][brigade] = sorted(
                        brigades[route][brigade],
                        key=lambda i: i["trip_id"].split("/")[-1]
                    )

        # Load data from API UM
        source += load_api_positions(apikey, "1") # Bus posiions
        source += load_api_positions(apikey, "2") # Tram positions

        # Iterate over results
        for v in source:
            # Read data about position
            lat, lon, route, brigade = v["Lat"], v["Lon"], v["Lines"], v["Brigade"].lstrip("0")
            tstamp = datetime.strptime(v["Time"], "%Y-%m-%d %H:%M:%S")
            trip_id = None
            bearing = None
            id = "/".join(["V", route, brigade])
            triplist = brigades.get(route, {}).get(brigade, [])
            if not triplist: continue

            # Do not care about obsolete data
            if (datetime.today() - tstamp) > timedelta(minutes=10): continue

            # Try to match with trip based on the difference where the vehicle was previously and where it is now
            if id in previous:
                prev_trip, prev_lat, prev_lon, prev_bearing = previous[id]["trip_id"], previous[id]["lat"], previous[id]["lon"], previous[id].get("bearing", None)
                tripidslist = [x["trip_id"] for x in triplist]

                # Get vehicle bearing
                bearing = initial_bearing([prev_lat, prev_lon], [lat, lon])
                if (not bearing) and prev_bearing: bearing = prev_bearing

                # If vehicle was doing its last trip, there's nothing more that can be calculated
                if prev_trip == triplist[-1]["trip_id"]:
                    trip_id = prev_trip

                # The calculations require for the prev_trip to be in the triplist
                elif prev_trip in tripidslist:
                    prev_trip_index = tripidslist.index(prev_trip)
                    prev_trip_last_latlon = list(map(float, triplist[prev_trip_index]["last_stop_latlon"]))

                    # If vehicle is near (50m) the last stop => the trip has finished => assume the next trip
                    # Or if the previous trip should've finished 30min earlier (A fallback rule if the previous cause has failed)
                    # FIXME: Some trips pass around last stop more then one time (see 146/TP-FAL-W) (a loop-the-loop near end terminus)
                    if haversine([lat, lon], prev_trip_last_latlon) <= 0.05 or \
                        later_in_time(triplist[prev_trip_index]["last_stop_timepoint"], (datetime.now()-timedelta(minutes=30)).strftime("%H:%M:%S")):
                        trip_id = triplist[prev_trip_index + 1]["trip_id"]
                    else:
                        trip_id = prev_trip

            # If this vehicle wasn't defined previously we have to assume it's running on time
            # (Or rather I'm to lazy to think of a way to match it to current trip_id, and the current algorithm works just fine)
            if not trip_id:
                currtime = datetime.now().strftime("%H:%M:%S")
                for trip in triplist:
                    if later_in_time(currtime, trip["last_stop_timepoint"]):
                        trip_id = trip["trip_id"]
                        break

            # If the vehicle has no active trips now - assume it's doing the last trip
            if not trip_id:
                trip_id = triplist[-1]["trip_id"]

            # Save to dict
            data = {
                "id": id,
                "trip_id": trip_id,
                "timestamp": tstamp,
                "lat": lat,
                "lon": lon
            }
            if bearing: data["bearing"] = bearing

            positions[id] = data

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
        if out_proto and binary_proto:
            with open("gtfs-rt/vehicles.pb", "wb") as f: f.write(container.SerializeToString())

        elif out_proto:
            with open("gtfs-rt/vehicles.pb", "w") as f: f.write(str(container))

        if out_json:
            for i in map(copy, positions.values()):
                i["timestamp"] = i["timestamp"].isoformat()
                json_container["positions"].append(i)
            with open("gtfs-rt/vehicles.json", "w", encoding="utf8") as f: json.dump(json_container, f, indent=2, ensure_ascii=False)

        return positions
