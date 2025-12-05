# WarsawGTFS

## Description

Create GTFS data for [Warsaw Public Transport](https://www.wtp.waw.pl/) buses, trams, metro and trains.

Written in [Python](https://www.python.org/) and with the [Impuls framework](https://impuls.readthedocs.io).

Static data comes from a proprietary endpoint shared by ZTM (the Public Transit Authority).
Access to it is restricted; credentials need to be provided through `WARSAW_ZTM_USER`, `WARSAW_ZTM_PASS` and `WARSAW_ZTM_KEY` env variables.

Realtime feeds incorporate data from <https://api.um.warszawa.pl>.

## Features

1. Line colors
2. Calendar exceptions
3. Trip headsigns and requests stops
4. Fares
5. Unambiguous stop names, prepended by town names when necessary
6. Deduplicated stops
7. Overrides for faulty stop data (<data_curated/stops.json>)
8. Better shapes based on [OSM](https://www.openstreetmap.org/) for buses and a custom graph for other modes
9. Frequency-based metro schedules (<data_curated/metro>)
10. Brigade numbers, planned vehicle types
11. Realtime feeds

## Creating static GTFS

1. Make sure you have a recent Python installation
2. Create a new [virtual environment](https://docs.python.org/3/library/venv.html) for dependency management: `python -m venv --upgrade-deps .venv`
3. Activate the venv: `. .venv/bin/activate`
4. Install dependencies: `pip install -Ur requirements.txt`
5. [Provide API keys](#api-keys)
6. Run the desired script, e.g. `python -m warsaw_gtfs`

See `python -m warsaw_gtfs -h` for all options; most notably `--shapes`.
Due to the sheer volume of data, the script can take a couple of minutes to run.

[Impuls](https://impuls.readthedocs.io) has a smart caching system and won't re-build the GTFS unless necessary.

## Creating realtime GTFS

The part of the project responsible for GTFS-Realtime has been written a long time ago
and is ugly and I plan to re-write everything _in the future_. However, it works for now.

The realtime code is written [go](https://golang.org/) and as such require the `go` command to be available.
You can run the main `warsawgtfs_realtime.go` script with `go run warsawgtfs_realtime.go` or by compiling it first with `go build warsawgtfs_realtime.go`.

There are several dependencies required by this project, all listed in the `go.mod` file.
AFAIK they should be downloaded automatically when running/compiling the project.
If they are not, `go mod download` explicitly downloads the dependencies.

Position data are mostly based on data from <https://api.um.warszawa.pl>,
while alerts are downloaded from <https://www.wtp.waw.pl/utrudnienia/> and <https://www.wtp.waw.pl/zmiany/>

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

### Positions

Creates the GTFS-Realtime feed with a vehicle positions and their active trips.

This mode is enabled by the `-p` command line flag. Here are all available options:

- `-k` (**required**): apikey to api.um.warszawa.pl
- `-json`: Apart from the GTFS-RT file, write the parsed positions into a custom JSON format
- `-readable`: Use a human-readable GTFS-RT format instead of the binary one
- `-gtfs-file SOME_FILE_OR_URL`: from which GTFS file should trips&brigades be loaded? defaults to <https://mkuran.pl/gtfs/warsaw.zip>
- `-target SOME_FOLDER`: where to put the created files? defaults to `data_rt`
- `-loop DURATION`: if positive (e.g. `30s`), updates the files every DURATION. defaults to `0s`, loop mode disabled.
- `-checkdata DURATION`: when in loop-mode, decides how often should the `-gtfs-file` be checked for changes. defaults to `30m`.

## License

*WarsawGTFS* is provided under the MIT license. Please take a look at the `license.md` file.
