"""
Constant values used all over the place.
Fare data is stored in fares/data.py.
"""

# cSpell: words rozklady

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

RAILWAY_MAP = "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"

URL_METRO_GTFS = "https://mkuran.pl/gtfs/warsaw/metro.zip"

# Logging attrivutes
LOGGING_STYLE = "{"
LOGGING_FMT = "[{levelname}] {name}: {message}"

# Pairs of stops that determine the direction_id of a train.
# If train calls at pair[0] before pair[1] - it shall have direction_id = 0;
# else if train calls at pair[1] before pair[0] - it shall have direction_id = 1.
RAIL_DIRECTION_STOPS = [
    ("4900", "2900"),  # W-wa Zachodnia      → W-wa Wschodnia
    ("5902", "7903"),  # W-wa Zachodnia p. 9 → W-wa Gdańska
    ("4905", "4900"),  # Pruszków            → W-wa Zachodnia  (S1 specific)
    ("2900", "2904"),  # W-wa Wschodnia      → W-wa Wawer      (S1 specific)
    ("2904", "2909"),  # W-wa Wawer          → W-wa Falenica   (S1 specific)
    ("2909", "2918"),  # W-wa Falenica       → Otwock          (S1 specific)
    ("2916", "2918"),  # Józefów             → Otwock          (S1 specific)
    ("3901", "4900"),  # W-wa Służewiec      → W-wa Zachodnia  (S2+S4 specific)
    ("2900", "2910"),  # W-wa Wschodnia      → W-wa Rembertów  (S2 specific)
    ("7903", "1907"),  # W-wa Gdańska        → Legionowo       (S9 specific)
    ("2900", "1907"),  # W-wa Wschodnia      → Legionowo       (S3 specific)
    ("7903", "1905"),  # W-wa Gdańska        → W-wa Płudy      (S3 specific)
    ("1907", "1910"),  # Legionowo           → Wieliszew       (S30 specific)
    ("3901", "4917"),  # W-wa Służewiec      → W-wa Rakowiec   (S2+S4 specific)
]

RAIL_STATION_ID_MIDDLES = frozenset({"90", "91", "92", "93"})


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
        "trip_short_name",
    ],

    "stop_times.txt": [
        "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
        "pickup_type", "drop_off_type", "shape_dist_traveled", "platform",
    ],
}
