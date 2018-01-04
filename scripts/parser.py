import re
import csv
import yaml
from .shapes import Shaper
import urllib.request as request
from urllib.parse import quote_plus
from codecs import decode
from bs4 import BeautifulSoup
from decimal import Decimal, getcontext

getcontext().prec = 8

class railStopWriteClass(object):
    def __init__(self, config):
        self.km = config["parseKM"]
        self.skm = config["parseSKM"]
        self.wkd = config["parseWKD"]
        self.wkdstops = ["7911", "4911", "4910", "4912", "4914", "4915", "4916"]
        self.kmstops = ["5905", "5906", "3904", "3905", "3906", "2919", "1920", "1927", "1918", "1921", "1922", "1923", "1924", "1925", "1926", "1911", "1919", "4922", "4921", "4920", "2920", "3902", "3902", "3903"]
    def det(self, stop_id):
        "Determine if stop should be written"
        if stop_id == "4913" and (self.km or self.skm or self.wkd):
            return(True)
        elif stop_id in self.kmstops:
            if self.km: return(True)
            else: return(False)
        elif stop_id in self.wkdstops:
            if self.wkd: return(True)
            else: return(False)
        elif self.km or self.skm:
            return(True)
        else:
            return(False)

class namedecapClass(object):
    def __init__(self, config):
        self.usewebsite = config["nameDecap"]
        self.ids = {}
        self.names = {}
    def fromid(self, id, name):
        if id in self.ids:
            return(self.ids[id])
        elif self.usewebsite:
            website = request.urlopen("http://m.ztm.waw.pl/rozklad_nowy.php?c=183&l=1&a=" + id[:4])
            soup = BeautifulSoup(decode(website.read()), "html.parser")
            tag = str(soup.find("div", id="RozkladHeader"))
            tagsearch = re.search(r"<h4>(.+)\s{1}\(", tag)
            if tagsearch:
                text = tagsearch.group(1)
            else:
                text = name.title()
        else:
            text = name
        text = text.replace(".", ". ").replace("-", " - ").replace("  "," ")
        text = text.rstrip()
        text = text.replace("Praga - Płd.", "Praga-Płd.").replace("PRAGA - PŁD.", "PRAGA-PŁD.")
        self.ids[id] = text
        self.names[name] = text
        return text
    def fromstr(self, name):
        if name in self.names:
            return(self.names[name])
        elif self.usewebsite:
            website = request.urlopen("http://m.ztm.waw.pl/rozklad_nowy.php?c=183&l=1&t=1&p=" + quote_plus(name))
            soup = BeautifulSoup(decode(website.read()), "html.parser")
            tag = str(soup.find("div", id="RozkladHeader"))
            tagsearch = re.search(r"<h4>(.+)\s{1}\(", tag)
            if tagsearch:
                text = tagsearch.group(1)
            else:
                text = name.title()
        else:
            text = name
        text = text.replace(".", ". ").replace("-", " - ").replace("  "," ")
        text = text.rstrip()
        text = text.replace("Praga - Płd.", "Praga-Płd.").replace("PRAGA - PŁD.", "PRAGA-PŁD.")
        self.names[name] = text
        return text

def pointInPath(lat, lon, path):
    "Checks if point is in path using the even-odd rule"
    pathlen = len(path)
    y = pathlen - 1
    z = False
    for x in range(pathlen):
        if ((path[x][1] > lon) != (path[y][1] > lon)) and (lat < (path[y][0] - path[x][0]) * (lon - path[x][1]) / (path[y][1] - path[x][1]) + path[x][0]):
            z = not z
        y = x
    return z

def avglist(inlist):
    "Returns string of average of strings in input list"
    inlist = list(map(Decimal, inlist))
    avgsum = Decimal("0")
    for x in inlist: avgsum += x
    return(str(avgsum/len(inlist)))

def stopZone(lat, lon):
    "Returns ZTM zone for given lat lon"
    lat, lon = map(float, [lat, lon])
    zone1 = [[52.148388984254, 21.188719510403], [52.151983382615, 21.214913963546], [52.153819968698, 21.219640015819], [52.158954108359, 21.234853505339], [52.153056379404, 21.248586415496], [52.167220145199, 21.262920140491], [52.182589837628, 21.260345219837], [52.190496317382, 21.2671902173], [52.194915966248, 21.284184693618], [52.210118306688, 21.261836528086], [52.222660266053, 21.255112230173], [52.250360969561, 21.270207702454], [52.253999672595, 21.268169223602], [52.255024235475, 21.252075969514], [52.262589505643, 21.250402271058], [52.267474723421, 21.190792857943], [52.278963369207, 21.172687947062], [52.287607432149, 21.174817621063], [52.285454778439, 21.16105252442], [52.283407035134, 21.14191228089], [52.306399331024, 21.137534915762], [52.311122297192, 21.122021018832], [52.314519897346, 21.129423915769], [52.323570146111, 21.148499786273], [52.367588271452, 21.144884168517], [52.370942233066, 21.131752073187], [52.367588271457, 21.116045057196], [52.361194076723, 21.108470498967], [52.337312490872, 21.084952890318], [52.367195212411, 21.073108255302], [52.367214865429, 21.028642594251], [52.364044062648, 21.005597054398], [52.360165415946, 20.970985829282], [52.362890987026, 20.955514847691], [52.35753796346, 20.931369602194], [52.37811421553942, 20.887928009033203], [52.379790828551016, 20.818920135498047], [52.3447783246691, 20.80209732055664], [52.307114367928, 20.870837509176], [52.288093077315, 20.867624222786], [52.275119927961, 20.870985030682], [52.25774643651, 20.863195895705], [52.255106330749, 20.86830282169], [52.248945472779, 20.863871812397], [52.24464944293, 20.870491504246], [52.240734044277, 20.868946551854], [52.231942847354, 20.880303024822], [52.227632027929, 20.884798407131], [52.218430708442, 20.87135517554], [52.215236090302, 20.870303749607], [52.2085831851, 20.85940325217], [52.203454771924, 20.852257847355], [52.195392761969, 20.85358822302], [52.192157025424, 20.856391131421], [52.182178688277, 20.867192387125], [52.182099747212, 20.879358887194], [52.181994492244, 20.891267895217], [52.17686301064, 20.902919411187], [52.173415375139, 20.917853950982], [52.167341886406, 20.919723450629], [52.16175459749, 20.928403078972], [52.155890151147, 20.944496333058], [52.147832682117, 20.963400542197], [52.140048664664, 20.983126848404], [52.137006454285, 20.98368474787], [52.129880810768, 20.983309238608], [52.103910645967, 20.984725444992], [52.103040759171, 21.014929800677], [52.096694026023, 21.015975862193], [52.097896877103, 21.022201269313], [52.100335434549, 21.023810594722], [52.112829301927, 21.043991535349], [52.099017311949, 21.083119600447], [52.101798505042, 21.116974442653], [52.103063824589, 21.118626683406], [52.116795504503, 21.129183858047], [52.129001551179, 21.136195152407], [52.131767702545, 21.137868850832], [52.146886294644, 21.17983469306], [52.148387338154, 21.18868598281]]
    zone2w = [[52.139019801948, 21.325072288099], [52.144089807948, 21.331767081801], [52.13137419518, 21.356100081987], [52.124392564139, 21.344985007875], [52.112165488697, 21.361550330733], [52.102992980938, 21.368674277867], [52.108264767682, 21.459998130385], [52.135668013574, 21.478537559095], [52.157999729626, 21.431159019047], [52.183688409183, 21.396483420904], [52.199473268765, 21.380690574232], [52.210834897503, 21.293486594743], [52.205785643381, 21.247481345722], [52.174004255582, 21.270827293002], [52.147256622696, 21.255377769077], [52.140830584592, 21.280526160774], [52.147572633412, 21.31185436207], [52.138828842349, 21.325072288096]]
    if pointInPath(lat, lon, zone1): return "1"
    elif pointInPath(lat, lon, zone2w): return "2w"
    else: return "2"

def tramColor(stoplist):
    "Returns a colour for a tram route."
    if [x for x in ["701105", "701106", "701307", "701308", "701607", "701608"] if x in stoplist]: return "C90000,FFFFFF" #Marszałkowska
    elif [x for x in ["700207", "700208", "701309", "701310", "700303", "700304"] if x in stoplist]: return "FFA200,000000" #Al. Jerozolimskie
    elif [x for x in ["700209", "700210", "708509", "708510", "708903", "708904"] if x in stoplist]: return "3D3DAE,FFFFFF" #Al. Jana Pawła II
    elif [x for x in ["708505", "708506", "500203", "500204"] if x in stoplist]: return "AAFE00,000000" #Al. Solidarności
    else: return "800080,FFFFFF" #Neither of above

#routeParsable = lambda x, y: True if len(x) < 3 else False

def routeParsable(rid, config):
    "Check if route should be parsed based on config and route_id"
    if (not config["parseKM"]) and (rid.startswith("R") or rid in ["ZB", "ZM", "ZG"]): return False
    elif (not config["parseWKD"]) and rid == "WKD": return False
    elif (not config["parseSKM"]) and rid.startswith("S"): return False
    else: return True

def routeTypeColor(rid, desc):
    "Get route_type, route_color and agency_id based on route description"
    desc = desc.lower()
    if "tram" in desc: return ("0", "000080,FFFFFF", "ztm")
    elif "kolei" in desc:
        if "dojazdowej" in desc: return ("2", "990099,FFFFFF", "wkd")
        elif "mazowieckich" in desc: return ("2", "008000,FFFFFF", "km")
        else: return ("2", "000080,FFFFFF", "ztm")
    elif rid in ["ZM", "ZB", "ZG"]: return ("3", ",", "km")
    elif "nocna" in desc: return ("3", "000000,FFFFFF", "ztm")
    elif "ekspresowa" in desc or "przyspieszona" in desc: return ("3", "B60000,FFFFFF", "ztm")
    elif "strefowa" in desc: return ("3", "006600,FFFFFF", "ztm")
    else: return ("3", ",", "ztm")

def tripHeadsigns(stop, stopNames):
    "Get trip_headsign based on last stop_id and its stop_name"
    if stop in ["503803", "503804"]: return "Zjazd do zajezdni Wola"
    elif stop == "103002": return "Zjazd do zajezdni Praga"
    elif stop == "324010": return "Zjazd do zajezdni Mokotów"
    elif stop in ["606107", "606108"]: return "Zjazd do zajezdni Żoliborz"
    elif stop == "420201": return "Lotnisko Chopina"
    else: return stopNames[stop[:4]]

def parse(fileloc, config):
    #Load Config
    decapNames = config["nameDecap"]
    getMissingStops = config["getMissingStops"]
    getRailwayPlatforms = config["getRailwayPlatforms"]
    parseSKM = config["parseSKM"]
    parseKM = config["parseKM"]
    parseWKD = config["parseWKD"]

    #Open Files
    file = open(fileloc, "r", encoding="windows-1250")

    fileRoutes = open("output/routes.txt", "w", encoding="utf-8", newline="")
    csvRoutes = csv.DictWriter(fileRoutes, fieldnames= \
                ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"])
    csvRoutes.writeheader()

    fileTrips = open("output/trips.txt", "w", encoding="utf-8", newline="")
    csvTrips = csv.DictWriter(fileTrips, fieldnames= \
               ["route_id", "service_id", "trip_id", "exceptional", "trip_headsign", "direction_id", "wheelchair_accessible", "bikes_allowed",  "shape_id"])
    csvTrips.writeheader()

    fileTimes = open("output/stop_times.txt", "w", encoding="utf-8", newline="")
    csvTimes = csv.DictWriter(fileTimes, fieldnames= \
               ["trip_id", "arrival_time", "departure_time", "stop_id", "original_stop_id", "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled"])
    csvTimes.writeheader()

    fileCalendars = open("output/calendar_dates.txt", "w", encoding="utf-8", newline="")
    csvCalendars = csv.DictWriter(fileCalendars, fieldnames= \
                   ["service_id", "date", "exception_type"])
    csvCalendars.writeheader()


    fileStops = open("output/stops.txt", "w", encoding="utf-8", newline="")
    csvStops = csv.DictWriter(fileStops, fieldnames= \
               ["stop_id", "stop_code", "stop_name", "zone_id", "stop_lat", "stop_lon", "wheelchair_boarding", "platform_code", "location_type", "parent_station"])
    csvStops.writeheader()

    fileBadStops = open("bad-stops.txt", "w", encoding="utf-8", newline="\r\n")

    #File Section Booleans
    inLL = False
    inTR = False
    inLW = False
    inRP = False
    inTD = False
    inWG = False
    inOD = False
    inWK = False
    inKA = False
    inZP = False
    inPR = False

    #Other Variables, for use later
    namedecap = namedecapClass(config)
    missingstops = {}
    incorrectStops = []
    notUsedMissingStops = []
    virtualStopsFixer = {}
    railNumbers = ["90", "91", "92"]
    railStopWrite = railStopWriteClass(config)
    railStops = {"names": {}, "lats": {}, "lons": {}}

    if config["shapes"]: shaper = Shaper(True)

    #Other Variables, used per one line
    trips = {}
    tripDirectionStops = {"A": [], "B": []}
    tripCommonDirections = []
    trip_direction = ""
    trip_position = ""
    tripsSorted = []
    tripStops = []
    tripsLowFloor = []
    stopsDemanded = []
    lowFloorTimes = []

    #Railway Stations data read
    if getRailwayPlatforms:
        railData = request.urlopen("https://gist.githubusercontent.com/MKuranowski/4ab75be96a5f136e0f907500e8b8a31c/raw")
        railData = yaml.load(decode(railData.read()), Loader=yaml.BaseLoader)
    else:
        railData = {}

    #Read File
    for line in file:
        line = line.lstrip().rstrip()

        if line.startswith("*") or line.startswith("#"): #Section Change
            if line.startswith("*LL"): #Lines
                inLL = True
            elif line.startswith("#LL"):
                inLL = False
                break #EOF
            elif line.startswith("*TR"): #Line Description
                inTR = True
            elif line.startswith("#TR"):
                tripDirectionStops["A"] = set(tripDirectionStops["A"])
                tripDirectionStops["B"] = set(tripDirectionStops["B"])
                tripDirectionStops["unique"] = tripDirectionStops["A"] ^ tripDirectionStops["B"]
                if parsable:
                    route_color_only, route_text_color = route_color.split(",")
                    csvRoutes.writerow({\
                        "route_id": route_id, "agency_id": agency, "route_short_name": route_id,
                        "route_long_name": route_name, "route_type": route_type,
                        "route_color": route_color_only, "route_text_color": route_text_color})
                inTR = False
            elif line.startswith("*LW"): #Pattern Description
                inLW = True
            elif line.startswith("#LW"):
                if route_type == "0" and trip_direction == "A" and trip_position == "0":
                    route_color = tramColor(tripStops)
                tripStops = []
                inLW = False
            elif line.startswith("*WG"): #Timetables
                inWG = True
            elif line.startswith("#WG"):
                inWG = False
            elif line.startswith("*OD"): #Departures
                inOD = True
            elif line.startswith("#OD"):
                lowFloorTimes = []
                inOD = False
            elif line.startswith("*WK"): #Stoptimes
                inWK = True
            elif line.startswith("#WK"):
                #Write StopTimes
                for trip_id in tripsSorted:
                    trip = trips[trip_id]
                    if len(trip) > 1:
                        if trip_id in tripsLowFloor or route_type != "0": trip_low = "1"
                        else: trip_low = "2"

                        if config["shapes"]:
                            shape_distances = shaper.get(trip_id, [i["stop"] for i in trip])
                            shape_id = trip_id.split("/")[0] + "/" + trip_id.split("/")[1] if shape_distances else ""
                        else:
                            shape_distances, shape_id = {}, ""

                        stops = set([i["original_stop"] for i in trip]) & tripDirectionStops["unique"]

                        direction_a_length = len(stops & tripDirectionStops["A"]) # trip_direction is determined by sharing common stops with main patterns
                        direction_b_length = len(stops & tripDirectionStops["B"])

                        if not stops:
                            # Trips with all bi-directional stops -> a lasso/circular trip
                            # Assume trip_direction = 0
                            trip_direction = "0"
                        elif direction_a_length >= direction_b_length:
                            trip_direction = "0"
                        elif direction_a_length < direction_b_length:
                            trip_direction = "1"
                        else:
                            trip_direction = ""
                            print("Can't find trip_direction for trip {}".format(trip_id))

                        unusual_trip = "0" if [i for i in tripCommonDirections if trip_id.split("/")[1].startswith(i)] else "1"

                        csvTrips.writerow({ \
                            "route_id": route_id, "service_id": trip_id.split("/")[2], "trip_id": trip_id,
                            "trip_headsign": tripHeadsigns(trip[-1]["stop"], namedecap.ids), "exceptional": unusual_trip,
                            "direction_id": trip_direction, "wheelchair_accessible": trip_low, "bikes_allowed": "1", "shape_id": shape_id})

                        sequence = 0
                        for stopt in trip:
                            sequence += 1
                            csvTimes.writerow({ \
                                "trip_id": trip_id, "arrival_time": stopt["time"], "departure_time": stopt["time"],
                                "stop_id": stopt["stop"], "original_stop_id": stopt["original_stop"],
                                "stop_sequence": sequence, "pickup_type": stopt["pickDropType"],
                                "drop_off_type": stopt["pickDropType"], "shape_dist_traveled": shape_distances.get(stopt["stop"], "")})

                trips = {}
                tripDirectionStops = {"A": [], "B": []}
                tripCommonDirections = []
                tripsSorted = []
                tripsLowFloor = []
                stopsDemanded = []
                inWK = False
            elif line.startswith("*KA"): #Calendar Dates
                inKA = True
            elif line.startswith("#KA"):
                inKA = False
            elif line.startswith("*ZP"): #Stop Groups
                #Missing Stops Import
                if getMissingStops:
                    missingstops_raw = request.urlopen("https://gist.githubusercontent.com/MKuranowski/05f6e819a482ccec606caa64573c9b5b/raw").readlines()
                    missingstops_raw = [str(x, "utf-8").rstrip() for x in missingstops_raw]
                    missingstops_headers = missingstops_raw[0].split(",")

                    for missingstop_raw in missingstops_raw[1:]:
                        missingstop = dict(zip(missingstops_headers, missingstop_raw.split(",")))
                        missingstops[missingstop["stop_id"]] = {"lat": missingstop["stop_lat"], "lon": missingstop["stop_lon"]}
                        notUsedMissingStops.append(missingstop["stop_id"])
                inZP = True
            elif line.startswith("#ZP"):
                #Railway Stops
                for stop_num in sorted(railStops["names"]):
                    #Railway Platforms Importer
                    if railStopWrite.det(stop_num):
                        if stop_num in railData:
                            data = railData[stop_num]
                            stop_name = data["name"]
                            stop_lat, stop_lon = data["pos"].split(",")
                            wheelchairs = data.get("wheelchair", "")
                            if config["shapes"]: shaper.stops[stop_num] = [stop_lat, stop_lon]
                            if data.get("platforms_unavailable", "false") == "true":
                                csvStops.writerow({"stop_id": stop_num, "stop_name": stop_name, \
                                         "zone_id": data["zone"], "stop_lat": stop_lat, "stop_lon": stop_lon,
                                         "wheelchair_boarding": wheelchairs})

                            elif data.get("oneplatform", "false") == "true":
                                csvStops.writerow({"stop_id": stop_num, "stop_name": stop_name, \
                                         "zone_id": data["zone"], "stop_lat": stop_lat, "stop_lon": stop_lon,
                                         "wheelchair_boarding": wheelchairs, "platform_code": "1"})

                            else:
                                csvStops.writerow({"stop_id": stop_num, "stop_name": stop_name, \
                                         "zone_id": data["zone"], "stop_lat": stop_lat, "stop_lon": stop_lon,
                                         "wheelchair_boarding": wheelchairs, "location_type": "1"})

                                for platform_id in sorted(data["platforms"]):
                                    platform_lat, platform_lon = data["platforms"][platform_id].split(",")
                                    platform_name = " peron ".join([data["name"], platform_id.split("p")[1]])
                                    if config["shapes"]: shaper.stops[platform_id] = [platform_lat, platform_lon]
                                    csvStops.writerow({"stop_id": platform_id, "stop_name": platform_name, "zone_id": data["zone"], \
                                             "stop_lat": platform_lat, "stop_lon": platform_lon, "wheelchair_boarding": wheelchairs,
                                             "platform_code": platform_id.split("p")[1], "location_type": "0", "parent_station": stop_num})

                        else:
                            stop_name = railStops["names"][stop_num]
                            stop_lat = avglist(railStops["lats"][stop_num])
                            stop_lon = avglist(railStops["lons"][stop_num])
                            stop_zone = "2" if stop_num == "1918" else stopZone(stop_lat, stop_lon)

                            if config["shapes"]: shaper.stops[stop_num] = [stop_lat, stop_lon]
                            csvStops.writerow({"stop_id": stop_num, "stop_name": stop_name, \
                                     "zone_id": stop_zone, "stop_lat": stop_lat, "stop_lon": stop_lon})

                        namedecap.ids[stop_num] = stop_name

                inZP = False
            elif line.startswith("*PR"): #Stops
                inPR = True
            elif line.startswith("#PR"):
                #Write stops from group
                for stop in stopsInGroup:
                    stop_id = stop_num + stop["ref"]
                    stop_nameref = " ".join([stop_name, stop["ref"]])
                    stop_zone = stopZone(stop["lat"], stop["lon"])
                    if config["shapes"]: shaper.stops[stop_id] = [stop["lat"], stop["lon"]]
                    csvStops.writerow({"stop_id": stop_id, "stop_name": stop_nameref, \
                            "stop_lat": stop["lat"], "stop_lon": stop["lon"], "zone_id": stop_zone})

                #Virtual Stops Fixer
                for invalid in stopsVirtualInGroup:
                    for valid in [x["ref"] for x in stopsInGroup]:
                        if valid[1] == invalid[1]:
                            virtualStopsFixer[stop_num+invalid] = stop_num+valid
                            break
                inPR = False

        else: #File Content

            ### CALENDAR DATES ###
            if inKA:
                splited = line.split()
                date = splited[0].replace("-", "")
                for service in splited[2:]:
                    csvCalendars.writerow({"service_id": service, "date": date, "exception_type": "1"})

            ### STOPS ###
            elif inZP:
                zpMatch = re.match(r"(\d{4})\s{3}((.+)(?:,)|(.{30}))\s+(.{2})\s{2}(.+)", line)
                if zpMatch:
                    stop_num = zpMatch.group(1)
                    stop_name = namedecap.fromid(stop_num, (zpMatch.group(3) or zpMatch.group(4).rstrip(",")))
                    stop_town = zpMatch.group(6).title()
                    stopsInGroup = []
                    stopsVirtualInGroup = []

                if inPR:
                    prMatch = re.match(r"(\d{4})(\d{2}).+Y=\s?([0-9.]+)\s+X=\s?([0-9.]+)", line)
                    prWrongMatch = re.match(r"(\d{4})(\d{2}).+Y=[y.]+\s+X=[x.]+", line)
                    if prMatch:
                        stop_ref = prMatch.group(2)
                        stop_lat = prMatch.group(3)
                        stop_lon = prMatch.group(4)
                        #Railway Stops Merger
                        if stop_num[1:3] in railNumbers:
                            if stop_num not in railStops["names"]: railStops["names"][stop_num] = stop_name
                            if stop_num not in railStops["lats"]: railStops["lats"][stop_num] = [stop_lat]
                            else: railStops["lats"][stop_num].append(stop_lat)
                            if stop_num not in railStops["lons"]: railStops["lons"][stop_num] = [stop_lon]
                            else: railStops["lons"][stop_num].append(stop_lon)
                        elif stop_ref[0] == "8":
                            stopsVirtualInGroup.append(stop_ref)
                        else:
                            stopsInGroup.append({"ref": stop_ref, "lat": stop_lat, "lon": stop_lon})

                    elif prWrongMatch:
                        stop_ref = prWrongMatch.group(2)
                        missingstops_import = missingstops.get(stop_num + stop_ref, None)
                        if stop_ref[0] == "8":
                            stopsVirtualInGroup.append(stop_ref)
                        elif missingstops_import:
                            stopsInGroup.append({"ref": stop_ref, "lat": missingstops_import["lat"], "lon": missingstops_import["lon"]})
                            notUsedMissingStops.remove(stop_num + stop_ref)
                        else:
                            incorrectStops.append(stop_num + stop_ref)

            ### ROUTES ###
            elif inLL:
                llMatch = re.match(r"Linia:\s*([A-Za-z0-9-]*)\s*-\s*(.*)", line)
                if llMatch:
                    route_id = llMatch.group(1)
                    route_desc = llMatch.group(2)
                    route_type, route_color, agency = routeTypeColor(route_id, route_desc)
                    parsable = routeParsable(route_id, config)
                    if parsable and config["shapes"]: shaper.nextRoute(route_id, route_type)
                elif inTR and parsable: #Trip Descriptions
                    trMatch = re.match(r"(\w{2}-\w+).*,\s+(.*),.*==>\s*(.*),\s*[\w|-]{2}\s*Kier.\s(\w{1})\s+Poz.\s(\d).*", line)
                    if trMatch:
                        tripCommonDirections.append(trMatch.group(1))
                        trip_origin = namedecap.fromstr(trMatch.group(2))
                        trip_dest = namedecap.fromstr(trMatch.group(3))
                        trip_direction = trMatch.group(4)
                        trip_position = trMatch.group(5)
                        if trip_direction == "A" and trip_position == "0": #Route Long Name
                            route_name = " — ".join([trip_origin, trip_dest])
                    elif inLW: #OnDemand stops + stoplist for trams to get line color
                        lwMatch = re.match(r".*(\d{6})\s*.+,\s*[\w|-]{2}\s+\d{2}\s+(NŻ|)", line)
                        if lwMatch:
                            if lwMatch.group(2) == "NŻ": stopsDemanded.append(lwMatch.group(1))
                            if lwMatch.group(1) not in tripStops: tripStops.append(lwMatch.group(1))
                            if lwMatch.group(1) not in tripDirectionStops[trip_direction]: tripDirectionStops[trip_direction].append(lwMatch.group(1))
                    elif inWG and route_type == "0": #Low Floor tram trips catcher - read timetable
                        wgMatch = re.match(r"G\s+\d+\s+(\d+):\s+(.+)", line)
                        if wgMatch:
                            wgHour = wgMatch.group(1)
                            wgMinutesString = wgMatch.group(2)
                            wgMinutes = re.findall(r"\[(\d{2})", wgMinutesString)
                            for x in wgMinutes:
                                lowFloorTimes.append(wgHour + "." + x)
                    elif inOD and route_type == "0": #Low Floor tram trips catcher - assign to trip_id
                        odMatch = re.match(r"(\d{1,2}.\d{2})\s+(.{17})", line)
                        if odMatch:
                            time = odMatch.group(1)
                            trip_id = "/".join([route_id, odMatch.group(2)])
                            if time in lowFloorTimes:
                                tripsLowFloor.append(trip_id)
                            else:
                                if int(time.split(".")[0]) >= 24:
                                    time = str(int(time.split(".")[0])-24) + time.split(".")[1]
                                    if time in lowFloorTimes:
                                        tripsLowFloor.append(trip_id)
                elif inWK and parsable: #StopTimes
                    wkMatch = re.match(r"(.{17})\s+(\d{6})\s(\w{2})\s+(\d+\.\d+)", line)
                    if wkMatch:
                        trip_id = "/".join([route_id, wkMatch.group(1)])
                        time = wkMatch.group(4).replace(".",":") + ":00"
                        stop = wkMatch.group(2)

                        #OnDemand Stops
                        if stop in stopsDemanded: pickDropType = "3"
                        else: pickDropType = "0"

                        #Some Stop ID Changes
                        if stop[1:3] in railNumbers: #Rail Stops
                            try:
                                stop = railData[stop[:4]]["stops"][stop]
                            except KeyError:
                                stop = stop[:4]

                        elif stop in virtualStopsFixer: #Virtual Stops
                            stop = virtualStopsFixer[stop]

                        #Append trips
                        if stop not in incorrectStops:
                            if trip_id not in tripsSorted: tripsSorted.append(trip_id)
                            if trip_id not in trips: trips[trip_id] = []
                            trips[trip_id].append({"time": time, "stop": stop, "original_stop": wkMatch.group(2), "pickDropType": pickDropType})

    #Write info about incorrect stops
    if incorrectStops:
        fileBadStops.write("Stops without location:\n")
        for stop in incorrectStops: fileBadStops.write(stop + "\n")
    if notUsedMissingStops:
        fileBadStops.write("Not used stops from missing stops importer:\n")
        for stop in notUsedMissingStops: fileBadStops.write(stop + "\n")

    #Close Files
    file.close()
    fileRoutes.close()
    fileTrips.close()
    fileTimes.close()
    fileCalendars.close()
    fileStops.close()
    fileBadStops.close()
