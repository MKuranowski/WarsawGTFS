import argparse
import os

from src import *

# A simple interface
if __name__ == "__main__":
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-a", "--alerts", action="store_true", required=False, dest="alerts", help="parse alerts into output-rt/")
    argprs.add_argument("-b", "--brigades", action="store_true", required=False, dest="brigades", help="parse brigades into output-rt/")
    argprs.add_argument("-p", "--positions", action="store_true", required=False, dest="positions", help="parse positions into output-rt/")
    argprs.add_argument("-k", "--key", default="", required=False, metavar="(apikey)", dest="key", help="apikey from api.um.warszawa.pl")

    argprs.add_argument("--gtfs-file", default="https://mkuran.pl/feed/ztm/ztm-latest.zip", required=False, dest="gtfs_path", help="path/URL to the GTFS file")
    argprs.add_argument("--brigades-file", default="https://mkuran.pl/feed/ztm/ztm-brigades.json", required=False, dest="brigades_path", help="path/URL to brigades JSON file (created by option -b)")

    argprs.add_argument("--json", action="store_true", default=False, required=False, dest="json", help="output additionally rt data to .json format (only --alerts and --postions)")
    argprs.add_argument("--readable", action="store_false", default=True, required=False, dest="binary_proto", help="output data to a human-readable pb buff instead of binary one (only --alerts and --postions)")

    args = argprs.parse_args()

    if not os.path.exists("gtfs-rt"): os.mkdir("gtfs-rt")

    if (args.brigades or args.positions) and (not args.key):
        raise ValueError("Apikey is required for brigades/positions")

    if args.alerts:
        print("Parsing alerts")
        Realtime.alerts(gtfs_location=args.gtfs_path, out_proto=True, binary_proto=args.binary_proto, out_json=args.json)

    if args.brigades and args.key:
        print("Parsing brigades")
        Realtime.brigades(apikey=args.key, gtfs_location=args.gtfs_path, export=True)

    if args.positions and args.key:
        print("Parsing positions")
        Realtime.positions(apikey=args.key, brigades=args.brigades_path, out_proto=True, binary_proto=args.binary_proto, out_json=args.json)
