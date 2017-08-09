# WarsawGTFS

## Description
Creates GTFS data feed for Warsaw.
Data comes from [ZTM Warszawa FTP server](ftp://rozklady.ztm.waw.pl/) and optionally [otp-pl.tk website](http://otp-pl.tk/).

## Fetures

1. Line colors
2. Calendar exceptions
3. Trip headsigns and On-Request stops
4. Stop names and trip headsigns proper casing downloader (from ZTM's website)
5. Merging railway stops into one
6. Geting railway platforms from [external gist](https://gist.github.com/MKuranowski/4ab75be96a5f136e0f907500e8b8a31c)
7. Geting missing stop positions from [external gist](https://gist.github.com/MKuranowski/05f6e819a482ccec606caa64573c9b5b)
8. Inserting metro schedules from [otp-pl.tk](http://otp-pl.tk)
9. Fares (ZTM Warszawa only)


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

## License

*WarsawGTFS* is provided under the MIT license. Please take a look at the `license.md` file.
