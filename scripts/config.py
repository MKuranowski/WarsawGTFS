import yaml
import os
params = {"nameDecap":"""
# Should the script try to download proper cased stop names from ZTM's website?
# Otherwise all names shown to user will be in all UPPER cased
nameDecap: false""", "getMissingStops": """
# Should missing stops be downloaded from gist avaible at https://gist.github.com/MKuranowski/05f6e819a482ccec606caa64573c9b5b ?
getMissingStops: true""", "parseWKD": """
# Should the script parse WKD schedules?
# The data does not include all stops.
# I recommend using GTFS feed avilable at https://mkuran.pl/feed/
parseWKD: false""", "parseSKM": """
# Should the script parse SKM schedules?
# This data is fine.
parseSKM: true""", "parseKM": """
# Should the script parse Koleje Mazowieckie schedules?
# The data does not include all stops and lines.
# To get full data you have to contact Koleje Mazowieckie.
parseKM: true""", "addMetro": """
# Should the script add Metro schedules from https://mkuran.pl/feed/metro ?
# This data has to be included in the same feed as ZTM schedules in order for fares to work.
addMetro: false""", "getRailwayPlatforms": """
# Should railway platforms be downloaded from gist available at https://gist.github.com/MKuranowski/4ab75be96a5f136e0f907500e8b8a31c ?
# Otherwise every railway station/halt will have only one entry in stops.txt
getRailwayPlatforms: true""", "shapes": """
# Should the script generate shapes from data avilable at https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm (for Tram and Rail) and OSM (for buses)?
# Routing on OSM graphs will be done via pyroutelib3
# This will have large influence on parse time
shapes: false
"""}

def create(missingParams):
    if not os.path.exists("config.yaml"):
        file = open("config.yaml", "w", encoding="utf-8")
        file.write("# WarsawGTFS config file\n")
    else:
        file = open("config.yaml", "a", encoding="utf-8")
    for param in missingParams:
        file.write(params[param] + "\n")

def load():
    if not os.path.exists("config.yaml"):
        create(list(params.keys()))
        print("Config file not found, creating a default one!")
        print("Please set it up to your needs and start WarsawGTFS again")
        return(None)
    else:
        config = yaml.load(open("config.yaml", "r", encoding="utf-8"))
        missingParams = [x for x in list(params.keys()) if x not in config]
        if missingParams == []:
            return(config)
        else:
            create(missingParams)
            print("Config file is missing some params")
            print("Please set them up to your needs and start WarsawGTFS again")
            return(None)
