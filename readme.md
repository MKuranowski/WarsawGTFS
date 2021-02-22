# WarsawGTFS

## Description
Creates GTFS data feed for Warsaw.
Static data comes from [ZTM Warszawa FTP server](ftp://rozklady.ztm.waw.pl/) and optionally [mkuran.pl website](https://mkuran.pl/).
Realtime feeds incorporate data from <https://api.um.warszawa.pl>.

## Fetures

1. Line colors
2. Calendar exceptions
3. Trip headsigns and On-Request stops
4. Fares
5. Adding town names to stop_name
6. Proper handling of virtual stakes
7. Geting railway platforms from [external gist](https://gist.github.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7#file-rail_platforms-json)
8. Geting missing stop positions from [external gist](https://gist.github.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7#file-missing_stops-json)
9. Inserting metro schedules from [mkuran.pl](https://mkuran.pl/gtfs/warsaw/)
10. Realtime data
11. Shapes generator: Buses based on [OSM Data](https://www.openstreetmap.org/), Rail/Tram based on [my own graph](https://mkuran.pl/gtfs/warsaw/tram-rail-shapes.osm).


## Static GTFS script

### Requirements

First of all you need [Python3](https://www.python.org) and several modules, included in `requirements.txt`, so run `pip3 intall -r requirements.txt`.

Then start the script with `python3 warsawgtfs.py`

### Configuration

Run `python3 warsawgts.py -h` to see all possible options with their descriptions.
All of those are optional.

### Creating GTFS

Run `python3 warsawgtfs.py` with desired command line options.
After some time (up to 1 min, or 15 mins with the `--shapes` option turned on) the `gtfs.zip` file should be created.


Produced GTFS feed has additional columns not included in standard GTFS specification:
- `is_data_source` in `attributions.txt` - to indicate that this attribution represents entity that provides data,
- `platform_code` in `stops.txt` - A platform identifier for (most) railway stops ([from Google Transit extensions](https://developers.google.com/transit/gtfs/reference/gtfs-extensions#station-platforms)),
- `stop_IBNR` and `stop_PKPPLK` in `stops.txt` - railway station ids shared by rail operators in Poland.
- `exceptional` in `trips.txt` - Value `1` indicates an *unusual* trip which does not follow common line's route (e.g. trips to depot) ([from Google Transit extensions](https://developers.google.com/transit/gtfs/reference/gtfs-extensions#trip-diversions)).


## Realtime GTFS script

These scripts are written in [go](https://golang.org/) and as such require the `go` command to be available.
You can run the main `warsawgtfs_realtime.go` script with `go run warsawgtfs_realtime.go` or by compiling it first with `go build`.

There are several dependencies required by this project, all listed in the `go.mod` file.
AFAIK they should be downloaded automatically when running/compiling the project.
If they are not, `go mod download` explicitly downloads the dependencies.

Brigades and positions feeds are mostly based on data from <https://api.um.warszawa.pl>.


### Alerts

Creates the GTFS-Realtime feed with a list of all known alerts.

This mode is enabled by the `-a` command line flag. Here are all available options:

- `-json`: Apart from the GTFS-RT file, write the parsed alerts into a custom JSON format
- `-readable`: Use a human-readable GTFS-RT format instead of the binary one
- `-strict`: Failing to get more data about an alert from the wtp.waw.pl causes an error, instead of being ignored
- `-gtfs-file SOME_FILE_OR_URL`: from which file should available routes be loaded? defaults to <https://mkuran.pl/gtfs/warsaw.zip>
- `-target SOME_FOLDER`: where to put the created files? defaults to `data_rt`
- `-loop DURATION`: if positive (e.g. `1m30s`), updates the files every DURATION. defaults to `0s`, loop mode disabled.
- `-checkdata DURATION`: when in loop-mode, decides how often should the `-gtfs-file` be checked for changes. defaults to `30m`.


### Brigades

Creates a file joning a brigade number to a list of trips (_for today only_), required for positions.

This mode is enabled by the `-b` command line flag. Here are all available options:

- `-k` (**required**): apikey to api.um.warszawa.pl
- `-strict`: any mismatches between api.um.warszawa.pl and gtfs data will become fatal, instead of being ignored
- `-gtfs-file SOME_FILE_OR_URL`: from which file should the trips be loaded? defaults to <https://mkuran.pl/gtfs/warsaw.zip>
- `-target SOME_FOLDER`: where to put the brigades.json file? defaults to `data_rt`


### Positions

Creates the GTFS-Realtime feed with a vehicle positions and their active trips.

This mode is enabled by the `-p` command line flag. Here are all available options:

- `-k` (**required**): apikey to api.um.warszawa.pl
- `-json`: Apart from the GTFS-RT file, write the parsed positions into a custom JSON format
- `-readable`: Use a human-readable GTFS-RT format instead of the binary one
- `-brigades-file SOME_FILE_OR_URL`: path/url to the brigades.json file created by the `-b` mode. defaults to <https://mkuran.pl/gtfs/warsaw/brigades.json>
- `-target SOME_FOLDER`: where to put the created files? defaults to `data_rt`
- `-loop DURATION`: if positive (e.g. `30s`), updates the files every DURATION. defaults to `0s`, loop mode disabled.
- `-checkdata DURATION`: when in loop-mode, decides how often should the `-brigades-file` be checked for changes. defaults to `30m`.


## License

*WarsawGTFS* is provided under the MIT license. Please take a look at the `license.md` file.
