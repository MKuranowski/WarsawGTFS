from tempfile import TemporaryFile
from datetime import datetime
import requests
import zipfile
import csv
import re
import io

ALERT_FLAGS = {"autobusy", "tramwaje", "skm", "kolej", "metro"}

def no_html(text):
    "Clean text from html tags"
    if text == "None": return ""
    else: return re.sub("<.*?>", "", text)

def alert_flags(alert_soup):
    "Get additional flags about the alert from icons, passed as BS4's soup"
    flags = set()
    for icon in alert_soup.find_all("td", class_="ico"):
        flags |= {i.get("title") for i in icon.find_all("img")}
    return flags.intersection(ALERT_FLAGS)

def alert_description(alert_soup):
    "Get alert description from BS4's soup. Returns a (plain_text, html) for every alert soup"
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
    clean_desc = no_html(desc_with_tags.replace("</p>", "\n").replace("<br/>", "\n").replace("<br>", "\n").replace("\xa0", " ").replace("  "," "))

    return clean_desc, desc_with_tags

def timepoint_in_trips(timepoint, route, stop, times):
    "Try find trip_id in times for given timepoint, route and stop"
    valid_times = [i for i in times if i["routeId"] == route and i["stopId"] == stop]
    valid_trips = [i for i in times if i["timepoint"] == timepoint]

    # If not found, try to add 24h to timepoint, to catch after-midnight trips
    if not valid_trips:
        next_timepoint = ":".join([str(int(timepoint.split(":")[0]) + 24), timepoint.split(":")[1], timepoint.split(":")[2]])
        valid_trips = [i for i in times if i["timepoint"] == next_timepoint]

    if valid_trips:
        return valid_trips[0]["tripId"]

def later_in_time(t1, t2):
    "Check if t2 happended after t1. Both should be strings HH:MM:SS."
    t1 = [int(x) for x in t1.split(":")]
    t2 = [int(x) for x in t2.split(":")]

    # Fix for after-midnight trips
    if t2[0] >= 24 and t1[0] <= 3:
        t1[0] += 24

    t1 = 3600*t1[0] + 60*t1[1] + t1[2]
    t2 = 3600*t2[0] + 60*t2[1] + t2[2]

    return t2 > t1

def parse_apium_response(api_response):
    """Parses a wierd response from api.um.warszawa.pl, they kinda seem to overcomplicate JSON"""
    result = []

    for item in api_response["result"]:

        item_dict = {}

        for kv_pair in item["values"]:

            # Each item has to have a 'key' and 'value' keys
            if "key" not in kv_pair or "value" not in kv_pair:
                continue

            # Convert "null" string to None
            # Beacuse why use JSON's null, when you can use a "null" string
            if kv_pair["value"] == "null":
                kv_pair["value"] = None

            item_dict[kv_pair["key"]] = kv_pair["value"]

        result.append(item_dict)

    return result

def load_api_positions(apikey, request_type):
    api_response = requests.get(
        "https://api.um.warszawa.pl/api/action/busestrams_get/",
        timeout = 5,
        params = {
            "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
            "apikey": apikey,
            "type": request_type,
    })
    api_response.raise_for_status()
    api_response = api_response.json()

    # Check if response from API UM is correct, and add it to positions list
    if type(api_response["result"]) is list:
        return api_response["result"]
    elif api_response.get("error") == "Błędny apikey lub jego brak":
        print("WarsawGTFS-RT: Incorrect apikey!")
    elif request_type == "1":
        print("WarsawGTFS-RT: Incorrect buses positions response")
        print(api_response)
    elif request_type == "2":
        print("WarsawGTFS-RT: Incorrect trams positions response")
        print(api_response)


class WarsawGtfs:
    def __init__(self, gtfs_location):
        self.routes = {"0": set(), "1": set(), "2": set(), "3": set()}
        self.stops = {}
        self.services = set()

        if gtfs_location.startswith("https://") or gtfs_location.startswith("ftp://") or gtfs_location.startswith("http://"):
            gtfs_request = requests.get(gtfsloc)
            self.gtfs = TemporaryFile()
            self.gtfs.write(gtfs_request.content)
            self.gtfs.seek(0)

        else:
            self.gtfs = open(gtfs_location, mode="rb")

        self.arch = zipfile.ZipFile(self.gtfs, mode="r")

    @classmethod
    def routes_only(cls, gtfs_location):
        self = cls(gtfs_location)
        self.list_routes()
        self.close()

        return self.routes

    def list_routes(self):
        with self.arch.open("routes.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                if row["route_type"] not in self.routes: continue
                else: self.routes[row["route_type"]].add(row["route_id"])

    def list_services(self):
        today = datetime.today().strftime("%Y%m%d")

        with self.arch.open("calendar_dates.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                if row["date"] == today: self.services.add(row["service_id"])

    def list_stops(self):
        with self.arch.open("stops.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                self.stops[row["stop_id"]] = [row["stop_lat"], row["stop_lon"]] # list, not tuple because of json module

    def list(self):
        self.list_stops()
        self.list_routes()
        self.list_services()

    def close(self):
        self.arch.close()
        self.gtfs.close()
