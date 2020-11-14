from typing import Mapping, Tuple

# Invalid shape ratios
OVERRIDE_SHAPE_RATIOS: Mapping[Tuple[str, str], float] = {
    ("103102", "103101"):    9, ("103103", "103101"):    9, ("207902", "201801"):  3.5,
    ("700609", "700614"): 18.5, ("102810", "102811"): 12.5, ("410201", "419902"):  3.7,
    ("600516", "607505"):  3.6, ("406404", "406401"):  7.1, ("120502", "120501"): 15.5,
    ("607506", "607501"): 14.3, ("600517", "607505"):  3.8, ("205202", "205204"):  7.6,
    ("201802", "226002"):  3.8, ("325402", "325401"):   24, ("400901", "400806"):  5.5,
    ("707602", "707603"): 10.5, ("124202", "124201"): 13.3, ("700214", "700211"):  9.0,
    ("600515", "607505"):  4.6, ("403601", "403602"): 10.5, ("105004", "115402"):  5.5,
    ("243801", "203903"):  5.2, ("301201", "301202"):  8.3, ("600514", "607505"):    4,
    ("424502", "405952"): 11.8, ("124001", "124003"): 17.5, ("124202", "143702"):  3.8,
    ("600513", "607505"):  4.6, ("703301", "703302"): 11.1, ("401505", "401560"):   10,
    ("100610", "100609"):   19, ("102502", "102504"):   15, ("102805", "102811"):  8.5,
    ("206101", "206102"): 12.4, ("415001", "405902"):  4.1, ("205202", "205203"):  8.4,
    ("434601", "415002"):  4.1, ("396001", "332101"):  4.8, ("102813", "102811"):    9,
    ("400806", "400901"):  7.7, ("428501", "434901"):  4.3, ("428501", "434903"):    5,
    ("700216", "700211"):  8.5,
}

# Bus router settings
BUS_ROUTER_SETTINGS = {
    "weights": {
        "motorway": 1.5, "trunk": 1.5, "primary": 1.4, "secondary": 1.3, "tertiary": 1.3,
        "unclassified": 1, "residential": 0.6, "living_street": 0.6, "track": 0.3, "service": 0.5
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
    "52.4221 21.1929", "52.4898 21.1421",
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
