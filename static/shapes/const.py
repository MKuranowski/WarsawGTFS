from ..const import _BASE_GIST

# Shape-generation external data
GIST_OVERRIDE_RATIOS = _BASE_GIST + "shapes_override_ratios.json"
GIST_FORCE_VIA = _BASE_GIST + "shapes_force_via.json"

# Bus router settings
BUS_ROUTER_SETTINGS = {
    "weights": {
        "motorway": 1.5, "trunk": 1.5, "primary": 1.4, "secondary": 1.3, "tertiary": 1.3,
        "unclassified": 1, "residential": 0.6, "living_street": 0.6, "track": 0.3, "service": 0.3
    },
    "access": ["access", "vehicle", "motor_vehicle", "psv", "bus", "routing:ztm"],
    "name": "bus"
}

# Sources for external graphs
URL_OVERPASS = "https://overpass-api.de/api/interpreter/"
URL_TRAM_TRAIN_GRAPH = "https://mkuran.pl/gtfs/warsaw/tram-rail-shapes.osm"

# Overpass queries
_OVERPASS_QUERY_BOUND_POLY = " ".join([
    "52.4455 20.6858", "52.376 20.6872",  "52.3533 20.7868", "52.2929 20.726",  "52.2694 20.6724",
    "52.2740 20.4465", "52.2599 20.4438", "52.2481 20.5832", "52.2538 20.681",  "52.1865 20.6786",
    "52.1859 20.7129", "52.1465 20.7895", "52.0966 20.783",  "52.0632 20.7222", "52.0151 20.7617",
    "51.9873 20.9351", "51.9269 20.9509", "51.9144 21.0226", "51.9322 21.1987", "51.9569 21.2472",
    "52.0463 21.2368", "52.1316 21.4844", "52.1429 21.4404", "52.2130 21.3814", "52.2622 21.3141",
    "52.2652 21.1977", "52.3038 21.173",  "52.3063 21.2925", "52.3659 21.3515", "52.3829 21.3001",
    "52.4221 21.1929", "52.4898 21.1421", "52.4934 20.9234"
])

OVERPASS_BUS_GRAPH = f'''
[bbox:51.9144,20.4438,52.5007,21.4844][out:xml];
(
    way["highway"="motorway"];
    way["highway"="motorway_link"];
    way["highway"="trunk"];
    way["highway"="trunk_link"];
    way["highway"="primary"];
    way["highway"="primary_link"];
    way["highway"="secondary"];
    way["highway"="secondary_link"];
    way["highway"="tertiary"];
    way["highway"="tertiary_link"];
    way["highway"="unclassified"];
    way["highway"="minor"];
    way["highway"="residential"];
    way["highway"="living_street"];
    way["highway"="service"];
);
way._(poly:"{_OVERPASS_QUERY_BOUND_POLY}");
>->.n;
<->.r;
(._;.n;.r;);
out;
'''

OVERPASS_STOPS_JSON = '''
[bbox:51.9144,20.4438,52.5007,21.4844][out:json];
node[public_transport=stop_position][network="ZTM Warszawa"];
out;
'''
