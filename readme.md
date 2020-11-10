# WarsawGTFS

## Description
Creates GTFS data feed for Warsaw.
Data comes from [ZTM Warszawa FTP server](ftp://rozklady.ztm.waw.pl/) and optionally [mkuran.pl website](https://mkuran.pl/).

## Fetures

1. Line colors
2. Calendar exceptions
3. Trip headsigns and On-Request stops
4. Added town names to stop_name
5. Merging railway stops into one
6. Geting railway platforms from [external gist](https://gist.github.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7#file-rail_platforms-json)
7. Geting missing stop positions from [external gist](https://gist.github.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7#file-missing_stops-json)
8. Inserting metro schedules from [mkuran.pl](https://mkuran.pl/gtfs/warsaw/)
10. Realtime data
11. Shapes generator: Buses based on [OSM Data](https://www.openstreetmap.org/), Rail/Tram based on [my own graph](https://mkuran.pl/gtfs/warsaw/tram-rail-shapes.osm).


## Static GTFS script

### First Launch

First of all you need [Python3](https://www.python.org) and several modules, included in `requirements.txt`, so run `pip3 intall -r requirements.txt`.

Then start the script with `python3 warsawgtfs.py`

### Configuration

Run `python3 warsawgts.py -h` to see all possible options with their descriptions.
All of those are optional.

### Creating GTFS

Run `python3 warsawgtfs.py` with desired command line options.
After some time (up to 1 min, or 15 mins with the `--shapes` option turned on) the `gtfs.zip` file should be created.


Produced GTFS feed has additional columns not included in standard GTFS specification:
- ~~`original_stop_id` in `stop_times.txt` - WarsawGTFS changes some stop_ids (especially for railway stops and xxxx8x virtual stops), so this column contains original stop_id as referenced in the ZTM file,~~
- `is_data_source` in `attributions.txt` - to indicate that this attribution represents entity that provides data,
- `platform_code` in `stops.txt` - A platform identifier for (most) railway stops ([from Google Transit extensions](https://developers.google.com/transit/gtfs/reference/gtfs-extensions#station-platforms)),
- `stop_IBNR` and `stop_PKPPLK` in `stops.txt` - railway station ids shared by rail operators in Poland.
- `exceptional` in `trips.txt` - Value `1` indicates an *unusual* trip which does not follow common line's route (e.g. trips to depot) ([from Google Transit extensions](https://developers.google.com/transit/gtfs/reference/gtfs-extensions#trip-diversions)).


## Realtime GTFS script

The `warsawgtfs_realtime.py` contains three realtime functions.

All reailtime data is created in `gtfs-rt/` directory.
By default only the binary protobuf file is created. Outputing human-readable representation can be done by adding the `--readable` flag to the script. An additional JSON file is added when the script sees flag `--json`.

- **Alerts** (option `-a` / `--alerts`):  
  Arguments:
  - (optional) `--gtfs-file PATH_OR_URL` - Location of GTFS feed to fetch a list of valid routes.
    Defaults to <https://mkuran.pl/gtfs/warsaw.zip>

- **Brigades** (option `-b` / `--brigades`):  
  Arguments:
  - `--key APIKEY` / `-k APIKEY` - The apikey to <https://api.um.warszawa.pl>,
  - (optional) `--gtfs-file PATH_OR_URL` - Location of GTFS feed to base brigades on.
    Defaults to <https://mkuran.pl/gtfs/warsaw.zip>
  
  Notes:
  - Always outputs only a json file,
  - Data is valid only on the date of creation - this process has to be run every day.


- **Positions** (option `-p` / `--position`):  
  Arguments:
  - `--key APIKEY` / `-k APIKEY` - The apikey to <https://api.um.warszawa.pl>,
  - (optional) `--brigades-file PATH_OR_URL` - Location of the brigades.json file.
    Defaults to <https://mkuran.pl/gtfs/warsaw/brigades.json>

  Notes:
  - This script assumes that all trips are running on time.
  - If you wish to update positions in a loop, please `from src import Realtime` and call
  `Realtime.positions()` on your own. It returns matched vehicles, which then can be provided again to
  `Realtime.positions()` for a slightly better accuracy of matching trip_ids (instead of assuming everything runs on time).

  Arguments of `Realtime.positions()`:
  - `apikey`: Apikey to <https://api.um.warszawa.pl>
  - `brigades`: Path/URL to brigades.json
  - `previous`: Dictionary of known vehicles, as returned by this function.
    If calling for the first time provide an empty dict, `{}`.


## License

*WarsawGTFS* is provided under the MIT license. Please take a look at the `license.md` file.
