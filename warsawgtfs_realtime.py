from google.transit import gtfs_realtime_pb2 as gtfs_rt
from collections import OrderedDict
from datetime import datetime, timedelta
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


# Some random Functions

def _DictFactory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description): d[col[0]] = row[idx]
    return(d)

def _FilterLines(rlist):
    for x in rlist:
        if x.startswith("R") or x.startswith("KM") or x in ["Z", "", "KM", "WKD", "POP", "INFO"]:
            while x in rlist: rlist.remove(x)
        elif x.startswith("M") and x not in ["M1", "M2"]:
            while x in rlist: rlist.remove(x)
            x.append("M1")
            x.append("M2")

def _CleanTags(html):
    if raw_html == "None": return ""
    else: return re.sub("<.*?>", "", raw_html)

def _AlertDesc(link):
    text = str(request.urlopen(link).read(), "utf-8")
    soup = BeautifulSoup(website.text, 'html.parser')
    descsoup = soup.find("div", id="PageContent")
    if descsoup != None:
        descsoup.table.decompose()
        descsoup.h4.decompose()
        for tag in descsoup.find_all("div", {'id':'PageInfo'}): tag.decompose()
        for tag in descsoup.find_all("div", {'class':'cb'}): tag.decompose()
        descwithtags = str(descsoup)
        descwithtags = descwithtags.replace("<br/>", "\n").replace("<br>", "\n").replace("\xa0", " ").replace("  "," ")
        return _CleanTags(descwithtags)
    else:
        return ""

def _FindTrip(timepoint, route, stop, times):
    times = list(filter(lambda x: x["routeId"] == route and x["stopId"] == stop, times))
    trips = list(filter(lambda x: x["timepoint"] == timepoint, times))
    if trips: return(trips[0]["tripId"])
    # If not found, try to add 24h to timepoint, to catch after midnight trips
    timepointAM = ":".join([str(int(timepoint.split(":")[0]) + 24), timepoint.split(":")[1], timepoint.split(":")[2]])
    trips = list(filter(lambda x: x["timepoint"] == timepointAM, times))
    if trips: return(trips[0]["tripId"])
    else:
        #print("Trip not found for R%s S%s T%s" % (route, stop, timepoint))
        return()

def _TimeDifference(t1, t2):
    "Check if t2 happended after t1"
    t1 = [int(x) for x in t1.split(":")]
    t2 = [int(x) for x in t2.split(":")]
    if t2[0] >= 24 and t1[0] <= 3: t1[0] += 24 # Fix for after-midnight trips
    if t1[0] < t2[0]: return(True)
    elif t1[0] == t2[0] and t1[1] < t2[1]: return(True)
    elif t1[0] == t2[0] and t1[1] == t2[1] and t1[2] <= t2[2]: return(True)
    else: return(False)

def _Distance(pos1, pos2):
    "Calculate the distance between pos1 and pos2 in kilometers"
    lat1, lon1, lat2, lon2 = map(math.radians, [pos1[0], pos1[1], pos2[0], pos[1]])
    lat, lon = lat2 - lat1, lon2 - lon1
    dist = 2 * 6371 * math.asin(math.sqrt(math.sin(lat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(lon * 0.5) ** 2))
    return(dist)

# Main Functions

def Alerts():
    # Container
    container = gtfs_rt.FeedMessage()
    feed = feedparser.parse("http://www.ztm.waw.pl/rss.php?l=1&IDRss=6")
    idenum = 0

    # Alerts
    for alert in feed.entries:
        idenum += 1
        lines = _FilterLines(re.split("[^0-9a-zA-Z-]+", alert.title[33:]))
        if lines:
            # Gather data
            link = _CleanTags(str(alert.link))
            title = _CleanTags(str(alert.description))
            try: desc = _AlertDesc(link)
            except: desc = ""
            # Append to gtfs_rt container
            entity = container.entity.add()
            entity.id = "-".join(["a", str(idenum)])
            alert = entity.alert
            alert.url.translation.add().text = link
            alert.header_text.translation.add().text = title
            if desc: alert.description_text.translation.add().text = desc
            for line in lines:
                selector = alert.informed_entity.add()
                selector.route_id = line

    # Header
    header = container.header
    header.gtfs_realtime_version = "1.0"
    header.incrementality = 0
    header.timestamp = round(datetime.today().timestamp())

    # Export
    with open("output-rt/alerts.pb", "w") as f:
        f.write(str(container))
    with open("output-rt/alerts.pbn", "wb") as f:
        f.write(container.SerializeToString())

def Brigades(apikey, gtfsloc="https://mkuran.pl/feed/ztm/ztm-latest.zip", export=False):
    # Variables
    gtfsServices = []
    gtfsRoutes = []
    brigades = OrderedDict()
    gtfsStops = {}
    tripLastTime = {}
    tripLastStop = {}
    previousTrip = ""
    apiCalls = 0
    today = datetime.today().strftime("%Y%m%d")

    # Initialize DataBase
    dbc = sqlite3.connect(":memory:")
    dbc.row_factory = _DictFactory
    db = dbc.cursor()
    db.execute("CREATE TABLE stoptimes (route_id varchar(255), trip_id varchar(255), stop_id varchar(255), timepoint varchar(255))")
    dbc.commit()

    # Download GTFS
    if gtfsloc.startswith("https://") or gtfsloc.startswith("ftp://") or gtfsloc.startswith("http://"):
        print("Downloading GTFS")
        request.urlretrieve(gtfsloc, "input/gtfs-rt.zip")
        gtfsloc = "input/gtfs-rt.zip"

    # Read GTFS
    print("Creating database")
    with zipfile.ZipFile("gtfs.zip") as gtfs:

        # Routes suitable for matching brigades
        with gtfs.open("routes.txt") as routes:
            for line in routes.readlines():
                line = str(line, "utf-8")
                if line.split(",")[4] in ["0", "3"]:
                    gtfsRoutes.append(line.split(",")[0])

        # Service_ids active today
        with gtfs.open("calendar_dates.txt") as calendars:
            for line in calendars.readlines():
                line = str(line, "utf-8")
                if line.split(",")[1] == today:
                    gtfsServices.append(line.split(",")[0])

        # Stops for additional information used in parsing vehicles locations
        with gtfs.open("stops.txt") as stops:
            for line in stops.readlines():
                line = str(line, "utf-8")
                if not line.startswith("stop_id"):
                    gtfsStops[line.split(",")[0]] = (line.split(",")[4], line.split(",")[5])

        with gtfs.open("stop_times.txt") as stoptimes:
            for line in stoptimes.readlines():
                line = str(line, "utf-8")
                if not line.startswith("trip_id"):
                    trip_id = line.split(",")[0]

                    # \/ This means previous trip has ended, and we can save additoanl information about that trip
                    if previousTrip and previousTrip != trip_id:
                        tripLastTime[previousTrip] = timepoint
                        tripLastStop[previousTrip] = gtfsStops[stop_id]
                    previousTrip = copy(trip_id)
                    timepoint = line.split(",")[2]
                    stop_id = line.split(",")[3]
                    route_id = trip_id.split("/")[0]
                    try: service_id = trip_id.split("/")[2]
                    except IndexError: service_id = ""
                    # Save only if route is suitable for matching and is active today
                    if route_id in gtfsRoutes and service_id in gtfsServices:
                            db.execute("INSERT INTO stoptimes VALUES (?,?,?,?)", (route_id, trip_id, stop_id, timepoint))
                            dbc.commit()

            # Save the last trip:
            tripLastTime[trip_id] = timepoint
            tripLastStop[trip_id] = stop_id

    dbc.commit()

    # Match trips to brigades
    print("Matching trips to brigades")
    while True:
        print("Geting next stop route pair   ", end="\r")
        db.execute("SELECT * FROM stoptimes")
        randomDbEntry = db.fetchone()
        if not randomDbEntry: break #If there's no more stop-route pairs
        else:
            # Get some basic data
            route_id = randomDbEntry["route_id"]
            stop_id = randomDbEntry["stop_id"]
            if route_id not in brigades: brigades[route_id] = {}

            # Call API UM
            print("Downloading R: %s S: %s       " % (route_id, stop_id), end="\r")
            apiCallValues = (apikey, stop_id[:4], stop_id[4:], route_id)
            apiCall = requests.get("https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&apikey=%s&busstopId=%s&busstopNr=%s&line=%s" % apiCallValues).text
            apiCalls += 1
            result = json.loads(apiCall)["result"]
            if type(result) is not list: incorrect_resopnse_from_um()

            # Iterate over result's departures
            print("Matching R: %s S: %s          " % (route_id, stop_id), end="\r")
            for value in result:
                # Get API timepoint and brigade
                for key in value["values"]:
                    if key["key"] == "brygada":
                        brigade = key["value"].lstrip("0")
                        if brigade not in brigades[route_id]:
                            brigades[route_id][brigade] = []
                    elif key["key"] == "czas":
                        timepoint = key["value"].lstrip("0")

                # Try to find timepoint in GTFS
                db.execute("SELECT * FROM stoptimes WHERE route_id=? AND stop_id=? AND timepoint=?", (route_id, stop_id, timepoint))
                valueTrip = db.fetchone()

                if not valueTrip: # If not found, try to add 24 to hours - this should catch after midnight timepoints
                    timepointAM = ":".join([str(int(timepoint.split(":")[0]) + 24), timepoint.split(":")[1], timepoint.split(":")[2]])
                    db.execute("SELECT * FROM stoptimes WHERE route_id=? AND stop_id=? AND timepoint=?", (route_id, stop_id, timepointAM))
                    valueTrip = db.fetchone()

                if valueTrip:
                    trip_id = valueTrip["trip_id"]
                    trip_data = OrderedDict([("trip_id", trip_id), ("last_stop_latlon", tripLastStop[trip_id]), ("last_stop_timepoint", tripLastTime[trip_id])])
                    brigades[route_id][brigade].append(trip_data)
                    db.execute("DELETE FROM stoptimes WHERE trip_id=?", (trip_id, ))
                    dbc.commit()

            # Remove all remaining times of current stop route pair
            db.execute("DELETE FROM stoptimes WHERE route_id=? AND stop_id=?", (route_id, stop_id))
            dbc.commit()

    # Sort everything
    print("\nSorting")
    for route in brigades:
        for brigade in brigades[route]:
            brigades[route][brigade] = sorted(brigades[route][brigade], key= \
                lambda x: x["trip_id"].split("/")[-1])
        brigades[route] = OrderedDict(sorted(brigades[route].items()))

    if export:
        print("Exporting")
        with open("output-rt/brigades.json", "w") as jsonfile:
            jsonfile.write(json.dumps(brigades, indent=2))
    return(brigades)

def Positions(apikey, brigades="https://mkuran.pl/feed/ztm/ztm-brigades.json", previous={}):
    # Variables
    container = gtfs_rt.FeedMessage()
    positions = OrderedDict()
    source = []

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
        id = "-".join([route, brigade])
        try: triplist = brigades[route][brigade]
        except KeyError: continue

        # Try to match with trip
        if id in previous:
            prev_trip, prev_lat, prev_lon = previous[id]["trip_id"], previous["lat"], previous["lon"]
            tripidslist = [x["trip_id"] for x in triplist]

            # If vehicle was doing its last trip, there's nothing more that can be calculated
            if prev_trip == triplist[-1]["trip_id"]:
                trip_id = copy(prev_trip)

            # The calculations require for the prev_trip to be in the triplist
            elif prev_trip in tripidslist:
                prev_trip_index = tripidslist.index(prev_trip)
                prev_trip_last_latlon = list(map(float, triplist[prev_trip_index]["last_stop_latlon"]))
                # If vehicle is near (50m) the last stop => the trip has finished => assume the next trip
                # Or if the previous trip should've finished 30min earlier (A fallback rule if the previous cause has failed)
                if dist([lat, lon], prev_trip_last_latlon) <= 0.05 or \
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
        data = {}
        data["trip_id"] = copy(trip_id)
        data["timestamp"] = copy(tstamp)
        data["id"] = copy(id)
        data["lat"] = copy(lat)
        data["lon"] = copy(lon)
        positions[id] = copy(data)

        # Save to gtfs_rt container
        entity = container.entity.add()
        entity.id = "-".join(["v", id])
        vehicle = entity.vehicle
        vehicle.trip.trip_id = trip_id
        vehicle.vehicle.id = id
        vehicle.position.latitude = float(lat)
        vehicle.position.longitude = float(lon)
        vehicle.timestamp = round(tstamp.timestamp())

    # Add gtfs-rt header
    header = container.header
    header.gtfs_realtime_version = "1.0"
    header.incrementality = 0
    header.timestamp = round(datetime.today().timestamp())

    # Export results
    with open("output-rt/vehicles.pb", "w") as f:
        f.write(str(container))
    with open("output-rt/vehicles.pbn", "wb") as f:
        f.write(container.SerializeToString())

    return(positions)

# A simple interface
if __name__ == "__main__":
    import argparse
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-a", "--alerts", action="store_true", required=False, dest="alerts", help="parse alerts into output-rt/")
    argprs.add_argument("-b", "--brigades", action="store_true", required=False, dest="brigades", help="parse brigades into output-rt/")
    argprs.add_argument("-p", "--positions", action="store_true", required=False, dest="positions", help="parse positions into output-rt/")
    argprs.add_argument("-k", "--key", default="", required=False, metavar="(apikey)", dest="key", help="apikey from api.um.warszawa.pl")
    args = vars(argprs.parse_args())
    if (args["brigades"] or args["positions"]) and (not args["key"]):
        print("Apikey is required for brigades/positions")

    if args["alerts"]:
        print("Parsing Alerts")
        Alerts()

    if args["brigades"] and args["key"]:
        print("Parsing brigades")
        Brigades(apikey=args["key"])

    if args["positions"] and args["key"]:
        print("Parsing positions")
        Positions(apikey=args["key"])
