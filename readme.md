# WarsawGTFS

## Description
Creates GTFS data feed for Warsaw.
Data comes from [ZTM Warszawa FTP server](ftp://rozklady.ztm.waw.pl/) and optionally [mkuran.pl website](https://mkuran.pl/).

## Fetures

1. Line colors
2. Calendar exceptions
3. Trip headsigns and On-Request stops
4. Stop names and trip headsigns proper casing downloader (from ZTM's website)
5. Merging railway stops into one
6. Geting railway platforms from [external gist](https://gist.github.com/MKuranowski/4ab75be96a5f136e0f907500e8b8a31c)
7. Geting missing stop positions from [external gist](https://gist.github.com/MKuranowski/05f6e819a482ccec606caa64573c9b5b)
8. Inserting metro schedules from [mkuran.pl](https://mkuran.pl/feed/metro)
9. Fares (ZTM Warszawa only)
10. Realtime data
11. Shapes generator: Buses based on [OSM Data](https://www.openstreetmap.org/), Rail/Tram based on [my own graph](https://mkuran.pl/feed/ztm/ztm-km-rail-shapes.osm).


## Running

### First Launch

First of all you need [Python3](https://www.python.org) and several modules, included in `requirements.txt`, so run `pip3 intall -r requirements.txt`.

Then start the script with `python3 warsawgtfs.py` - this will create the default `config.yaml` file

### Configuration

**config.yaml**:
This file has serveral settings for data processing.
Each option comes with its own description in the file.

**Command line options**:
Run `python3 warsawgts.py -h` to see all possible options with their descriptions.
All of those are optional.

### Creating GTFS

After setting up `config.yaml`, run `python3 warsawgtfs.py` with desired command line options.
After some time (up to 1 min, or 15 mins with the nameDecap turned on) the `gtfs.zip` file should be created.


Produced GTFS feed has three additional columns not included in standard GTFS specification:
- `original_stop_id` in `stop_times.txt` - WarsawGTFS changes some stop_ids (especially for railway stops and xxxx8x virtual stops), so this column contains original stop_id as referenced in the ZTM file,
- `platform_code` in `stops.txt` - A platform identifier for (most) railway stops ([from Google Transit extensions](https://developers.google.com/transit/gtfs/reference/gtfs-extensions#station-platforms)),
- `exceptional` in `trips.txt` - Value `1` indicates an *unusual* trip which does not follow common line's route (e.g. trips to depot) ([from Google Transit extensions](https://developers.google.com/transit/gtfs/reference/gtfs-extensions#trip-diversions)).


## Realtime data

The `warsawgtfs_realtime.py` contains three realtime functions.

All rt data is created in `output-rt/` directory.

A `.pb` file contains a human-readable respresentation of `.pbn` (binary) GTFS-RT file.

- **Alerts()**
  - (No arguments required),
  - Only a one-time parse - you have to run it every 30s/60s, or any other desired interval.


- **Brigades()**
  - *apikey* (String) - The apikey to https://api.um.warszawa.pl,
  - *gtfsloc* (String) - Location of GTFS feed, can be a URL or a path,
  - *export* (Boolean) - Output brigades to a json file,
  - Returns an OrderedDict with mapping of brigades to trip_ids,
  - Data is valid only on the date of creation - this process has to be run every day.


- **Positions()**
  - *apikey* (String) - The apikey to https://api.um.warszawa.pl,
  - *brigades* (Dict/OrderedDict or String) - Dict of brigades table, or path/URL to JSON file with them,
  - *previous* (Dict) - The dict of previous positions, as returned by this function (needed to figure out the trip_id, otherwise assumes all trip are on shedule),
  - Returns a dict of all positions,
  - Only a one-time parse - you have to run it every 30s/60s, or any other desired interval.


## License

*WarsawGTFS* is provided under the MIT license. Please take a look at the `license.md` file.
