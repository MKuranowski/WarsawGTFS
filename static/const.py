"""
Constant values used all over the place.
Fare data is stored in fares/data.py.
"""


# Directories
DIR_DOWNLOAD = "data_src"
DIR_CONVERTED = "data_feeds"
DIR_SINGLE_FEED = "data_gtfs"
DIR_SHAPE_ERR = "err_shapes"
DIR_SHAPE_CACHE = "data_shapes"

# How long to keep Overpass data cached (in minutes)
SHAPE_CACHE_TTL = 2 * 1440

# External data sources
FTP_ADDR = "rozklady.ztm.waw.pl"

_BASE_GIST = "https://raw.githubusercontent.com/MKuranowski/WarsawGTFS/master/data_curated/"
GIST_MISSING_STOPS = _BASE_GIST + "missing_stop_locations.json"
GIST_RAIL_PLATFORMS = _BASE_GIST + "rail_platforms.json"
GIST_STOP_NAMES = _BASE_GIST + "stop_names.json"

URL_METRO_GTFS = "https://mkuran.pl/gtfs/warsaw/metro.zip"

# Logging attrivutes
LOGGING_STYLE = "{"
LOGGING_FMT = "[{levelname}] {name}: {message}"

# List of rail stops used by S× lines. Other rail stops are ignored.
ACTIVE_RAIL_STATIONS = {
    "4900", "4901", "7900", "7901", "7902", "2901", "2900", "2918", "2917", "2916", "2915",
    "2909", "2908", "2907", "2906", "2905", "2904", "2903", "2902", "4902", "4903", "4923",
    "4904", "4905", "2914", "2913", "2912", "2911", "2910", "4919", "3901", "4918", "4917",
    "4913", "1910", "1909", "1908", "1907", "1906", "1905", "1904", "1903", "1902", "1901",
    "7903", "5908", "5907", "5904", "5903", "5902", "1913", "1914", "1915",
}

# Irregular stop names
PROPER_STOP_NAMES = {
    "1226": "Mańki-Wojody",
    "1484": "Dom Samotnej Matki",
    "1541": "Marki Bandurskiego I",
    "2005": "Praga-Płd. - Ratusz",
    "2296": "Szosa Lubelska",
    "2324": "Wiązowna",
    "4040": "Lotnisko Chopina",
    "4305": "Posag 7 Panien",  # theoretically "Zajezdnia Ursus Płn."
    "4400": "Mobilis Sp. z.o.o.",
    "5001": "Połczyńska - Parking P+R",
    "6201": "Lipków Paschalisa-Jakubowicza",
}

# GTFS headers
HEADERS = {
    "agency.txt": [
        "agency_id", "agency_name", "agency_url", "agency_timezone",
        "agency_lang", "agency_phone", "agency_fare_url",
    ],

    "attributions.txt": [
        "attribution_id", "organization_name", "is_producer", "is_operator",
        "is_authority", "is_data_source", "attribution_url",
    ],

    "feed_info.txt": ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version"],

    "calendar_dates.txt": ["service_id", "date", "exception_type"],

    "fare_attributes.txt": [
        "fare_id", "price", "currency_type", "payment_method", "transfers",
        "agency_id", "transfer_duration",
    ],

    "fare_rules.txt": ["fare_id", "route_id", "contains_id"],

    "shapes.txt": [
        "shape_id", "shape_pt_sequence", "shape_dist_traveled", "shape_pt_lat", "shape_pt_lon"
    ],

    "stops.txt": [
        "stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station",
        "zone_id", "stop_IBNR", "stop_PKPPLK", "platform_code", "wheelchair_boarding",
    ],

    "routes.txt": [
        "agency_id", "route_id", "route_short_name", "route_long_name", "route_type",
        "route_color", "route_text_color", "route_sort_order",
    ],

    "trips.txt": [
        "route_id", "service_id", "trip_id", "trip_headsign", "direction_id",
        "shape_id", "exceptional", "wheelchair_accessible", "bikes_allowed",
    ],

    "stop_times.txt": [
        "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
        "pickup_type", "drop_off_type", "shape_dist_traveled",
    ],
}
