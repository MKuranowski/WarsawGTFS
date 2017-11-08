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
            for row in metro_lines[1:]:
                writer.writerow(dict(zip(metro_header, row.split(","))))

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

def feedinfo(ztm_path):
    version = ztm_path.lstrip("input/").rstrip(".TXT")
    file = open("output/feed_info.txt", 'w', encoding="utf-8", newline="\r\n")
    file.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
    file.write("ZTM Warszawa; MKuranowski,https://github.com/MKuranowski/WarsawGTFS,pl,%s\n" % version)
    file.close()

def fare():
    rules = open("output/fare_rules.txt","w", encoding="utf-8", newline="\r\n")
    attribs = open("output/fare_attributes.txt", "w", encoding="utf-8", newline="\r\n")
    #Read routes
    routes = []
    routeFile  = open("output/routes.txt", "r", encoding="utf-8", newline="\r\n")
    for line in routeFile:
        route_id, agency = line.split(",")[0], line.split(",")[1]
        if route_id != "route_id" and agency == "ztm":
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
        if not (route.startswith("L") or  route.startswith("R") or route == "WKD"):
            rules.write("Czasowy/20min,1," + route + "\n")
            rules.write("Czasowy/20min,2," + route + "\n")
            rules.write("Czasowy/20min,2w," + route + "\n")
    for route in routes: #Jednorazowy 1
        if not (route.startswith("L") or  route.startswith("R") or route == "WKD"):
            rules.write("Jednorazowy-Strefa1,1," + route + "\n")
    for route in routes: #Jednorazowy 1&2
        if not (route.startswith("L") or  route.startswith("R") or route == "WKD"):
            rules.write("Jednorazowy-Strefa1i2,1," + route + "\n")
            rules.write("Jednorazowy-Strefa1i2,2," + route + "\n")
            rules.write("Jednorazowy-Strefa1i2,2w," + route + "\n")
    for route in routes: #75min
        if not (route.startswith("L") or  route.startswith("R") or route == "WKD"):
            rules.write("Przesiadkowy/75min-Strefa1,1," + route + "\n")
    for route in routes: #90min
        if not (route.startswith("L") or  route.startswith("R") or route == "WKD"):
            rules.write("Przesiadkowy/90min-Strefa1i2,1," + route + "\n")
            rules.write("Przesiadkowy/90min-Strefa1i2,2," + route + "\n")
            rules.write("Przesiadkowy/90min-Strefa1i2,2w," + route + "\n")
    #24h
    rules.write("Dobowy/24h-Strefa1,1,\n")
    rules.write("Dobowy/24h-Strefa1i2,1,\n")
    rules.write("Dobowy/24h-Strefa1i2,2,\n")
    rules.write("Dobowy/24h-Strefa1i2,2w,\n")

    # "Local" (Lxx) lines
    l2 = True
    l3 = True
    l36 = True
    l4 = True
    l5 = True

    #Prices for local lines:
    localPrices = {
    "L-2zl": ["L-1", "L-3", "L-4", "L-6", "L-7", "L18", "L26", "L27", "L29", "L35", "L36", "L37", "L38"],
    "L-3zl": ["L-8", "L-9", "L10", "L11", "L20", "L22", "L31", "L40"],
    "L-3.6zl": ["L14", "L15", "L16", "L21", "L28", "L30"],
    "L-4zl": ["L-2", "L-5", "L12", "L13", "L20", "L22", "L23", "L24", "L25", "L32", "L39"],
    "L-5zl": ["L17", "L19"]}
    for route in localPrices["L-2zl"]:
        if route in routes:
            if l2:
                attribs.write("L-2zl,2.00,PLN,0,0,\n")
                l2 = False
            rules.write("L-2zl,," + route + "\n")
    for route in localPrices["L-3zl"]:
        if route in routes:
            if l3:
                attribs.write("L-3zl,3.00,PLN,0,0,\n")
                l3 = False
            if route in ["L20", "L22"]:
                rules.write("L-3zl,2," + route + "\n")
            else:
                rules.write("L-3zl,," + route + "\n")
    for route in localPrices["L-3.6zl"]:
        if route in routes:
            if l36:
                attribs.write("L-3.6zl,3.60,PLN,0,0,\n")
                l36 = False
            rules.write("L-3.6zl,," + route + "\n")
    for route in localPrices["L-4zl"]:
        if route in routes:
            if l4:
                attribs.write("L-4zl,4.00,PLN,0,0,\n")
                l4 = False
            if route in ["L20", "L22"]:
                rules.write("L-4zl,2," + route + "\n")
                rules.write("L-4zl,2w," + route + "\n")
            else:
                rules.write("L-4zl,," + route + "\n")
    for route in localPrices["L-5zl"]:
        if route in routes:
            if l5:
                attribs.write("L-5zl,5.00,PLN,0,0,\n")
                l5 = False
            rules.write("L-5zl,," + route + "\n")
    rules.close()
    attribs.close()

def compress():
    archive = zipfile.ZipFile("gtfs.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
    for file in os.listdir("output"):
        if file.endswith(".txt"):
            archive.write("output/" + file, arcname=file)
    archive.close()
