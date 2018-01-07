from collections import OrderedDict
import urllib.request
import zipfile
import zlib
import csv
import os

def _FieldNames(f):
    r = csv.DictReader(f)
    return r.fieldnames

def _RewriteFile(filename, metrofile):
    gtfs_fileloc = os.path.join("output", filename)

    if os.path.exists(gtfs_fileloc):
        # Get gtfs file header
        with open(gtfs_fileloc, "r", encoding="utf-8", newline="") as f:
            gtfs_fieldnames = _FieldNames(f)

        # Decode metrofile
        metro_lines = [str(x, "utf-8").rstrip() for x in metrofile.readlines()]
        metro_header = metro_lines[0].split(",")

        # Append to gtfs - csv module is to keep columns aligned
        with open(gtfs_fileloc, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=gtfs_fieldnames)
            for row_raw in metro_lines[1:]:
                row = dict(zip(metro_header, row_raw.split(",")))
                if filename == "trips.txt" and not row.get("exceptional", ""):
                    row["exceptional"] = "0"
                writer.writerow(row)

    else:
        # If file does not exist then simply copy it, without caring about the content
        with open(gtfs_fileloc, "a", encoding="utf-8", newline="\r\n") as f:
            for row in metrofile:
                row = str(row, "utf-8")
                f.write(row.rstrip() + "\n")

def addMetro():
    urllib.request.urlretrieve("https://mkuran.pl/feed/metro/metro-latest.zip", "input/metro.zip")
    archive = zipfile.ZipFile("input/metro.zip")
    files = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar.txt", \
             "calendar_dates.txt", "frequencies.txt", "shapes.txt"]
    for filename in files:
        with archive.open(filename) as metrofile:
            _RewriteFile(filename, metrofile)
    archive.close()


def agency(config):
    file = open("output/agency.txt", 'w', encoding='utf-8', newline="\r\n")
    file.write("agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n")
    file.write("ztm,ZTM Warszawa,http://www.ztm.waw.pl,Europe/Warsaw,pl,19115,http://www.ztm.waw.pl/?c=110&l=1\n")
    if config["parseKM"]: file.write("km,Koleje Mazowieckie,http://www.mazowieckie.com.pl/,Europe/Warsaw,pl,+48223644444,http://www.mazowieckie.com.pl/pl/ceny-bilet-w#site\n")
    if config["parseWKD"]: file.write("wkd,Warszawska Kolej Dojazdowa,http://wkd.com.pl,Europe/Warsaw,pl,+48227557082,http://www.wkd.com.pl/bilety/ceny-biletow.html\n")
    file.close()

def feedinfo(ztm_path, shapes):
    version = ztm_path.lstrip("input/").rstrip(".TXT")
    file = open("output/feed_info.txt", 'w', encoding="utf-8", newline="\r\n")
    file.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
    if shapes: file.write("Data: ZTM Warszawa; GTFS Convert: MKuranowski; Bus Shapes (under ODbL License): © OpenStreetMap Contributors,https://github.com/MKuranowski/WarsawGTFS,pl,%s\n" % version)
    else: file.write("Data: ZTM Warszawa; GTFS Convert: MKuranowski,https://github.com/MKuranowski/WarsawGTFS,pl,%s\n" % version)
    file.close()

def fare():
    rules = open("output/fare_rules.txt","w", encoding="utf-8", newline="\r\n")
    attribs = open("output/fare_attributes.txt", "w", encoding="utf-8", newline="\r\n")
    #Read routes
    routes = []
    routeFile = open("output/routes.txt", "r", encoding="utf-8", newline="")
    routesReader = csv.DictReader(routeFile)
    for row in routesReader:
        route_id, agency = row["route_id"], row["agency_id"]
        if agency == "ztm":
            routes.append(route_id)
    routeFile.close()

    #Attributes
    attribs.write("fare_id,price,currency_type,payment_method,transfers,transfer_duration\n")
    attribs.write("Czasowy/20min,3.40,PLN,0,,1200\n")
    attribs.write("Jednorazowy-Strefa1,4.40,PLN,0,0,\n")
    attribs.write("Jednorazowy-Strefa1i2,7.00,PLN,0,0,\n")
    attribs.write("Przesiadkowy/75min-Strefa1,4.40,PLN,0,,4500\n")
    attribs.write("Przesiadkowy/90min-Strefa1i2,7.00,PLN,0,,5400\n")
    attribs.write("Dobowy/24h-Strefa1,15.00,PLN,0,,86400\n")
    attribs.write("Dobowy/24h-Strefa1i2,26.00,PLN,0,,86400\n")

    #Rules
    rules.write("fare_id,contains_id,route_id\n")
    for route in routes: #20min
        if not route.startswith("L"):
            rules.write("Czasowy/20min,1," + route + "\n")
            rules.write("Czasowy/20min,2," + route + "\n")
            rules.write("Czasowy/20min,2w," + route + "\n")

    for route in routes: #Jednorazowy 1
        if not route.startswith("L"):
            rules.write("Jednorazowy-Strefa1,1," + route + "\n")

    for route in routes: #Jednorazowy 1&2
        if not route.startswith("L"):
            rules.write("Jednorazowy-Strefa1i2,1," + route + "\n")
            rules.write("Jednorazowy-Strefa1i2,2," + route + "\n")
            rules.write("Jednorazowy-Strefa1i2,2w," + route + "\n")

    for route in routes: #75min
        if not route.startswith("L"):
            rules.write("Przesiadkowy/75min-Strefa1,1," + route + "\n")

    for route in routes: #90min
        if not route.startswith("L"):
            rules.write("Przesiadkowy/90min-Strefa1i2,1," + route + "\n")
            rules.write("Przesiadkowy/90min-Strefa1i2,2," + route + "\n")
            rules.write("Przesiadkowy/90min-Strefa1i2,2w," + route + "\n")
    #24h
    rules.write("Dobowy/24h-Strefa1,1,\n")
    rules.write("Dobowy/24h-Strefa1i2,1,\n")
    rules.write("Dobowy/24h-Strefa1i2,2,\n")
    rules.write("Dobowy/24h-Strefa1i2,2w,\n")

    # "Local" (Lxx) lines
    localPrices = {"L-2zl": "2.00", "L-3zl": "3.00", \
                  "L-3.6zl": "3.60", "L-4zl": "4.00", "L-5zl": "5.00"}

    # Prices for local lines:
    localFares = OrderedDict()
    localFares["L-2zl"] = ["L-1", "L-3", "L-4", "L-6", "L-7", "L18", "L26", "L27", "L29", "L35", "L36", "L37", "L38"]
    localFares["L-3zl"] = ["L-8", "L-9", "L10", "L11", "L20", "L22", "L31", "L40", "L41"]
    localFares["L-3.6zl"] = ["L14", "L15", "L16", "L21", "L28", "L30"]
    localFares["L-4zl"] = ["L-2", "L-5", "L12", "L13", "L20", "L22", "L23", "L24", "L25", "L32", "L39"]
    localFares["L-5zl"] = ["L17", "L19"]

    for fare_id, route_names in localFares.items():
        routesForFare = [x for x in routes if x.split("/")[0] in route_names]
        if routesForFare:
            attribs.write(",".join([fare_id, localPrices[fare_id], "PLN", "0", "0", ""]) + "\n")
        for route in routesForFare:
            if route in ["L20", "L22"] and fare_id == "L-3zl":
                rules.write(",".join([fare_id, "2", route]) + "\n")
            elif route in ["L20", "L22"] and fare_id == "L-4zl":
                rules.write(",".join([fare_id, "2", route]) + "\n")
                rules.write(",".join([fare_id, "2w", route]) + "\n")
            else:
                rules.write(",".join([fare_id, "", route]) + "\n")

    rules.close()
    attribs.close()

def compress():
    archive = zipfile.ZipFile("gtfs.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
    for file in os.listdir("output"):
        if file.endswith(".txt"):
            archive.write("output/" + file, arcname=file)
    archive.close()
