import argparse
import time

from src import *

if __name__ == "__main__":
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-s", "--shapes", action="store_true", required=False, dest="shapes", help="generate shapes based on OSM data. available only on Unix systems")
    argprs.add_argument("-m", "--metro", action="store_true", required=False, dest="metro", help="append metro schedules from mkuran.pl")

    argprs.add_argument("-p", "--prevver", default="", required=False, metavar="RAyymmdd", dest="prevver", help="previous feed_version, if you want to avoid downloading the same file again (ignored with --merge)")
    argprs.add_argument("-v", "--version", default="", required=False, metavar="RAyymmdd", dest="version", help="target version for which GTFS should be created (ignored with --merge)")

    argprs.add_argument("--merge", action="store_true", required=False, dest="merge", help="automatically create and merge all future schedules")
    argprs.add_argument("--remerge", action="store_true", required=False, dest="remerge", help="force merge of multi-day file (only valid with --merge)")
    argprs.add_argument("--reparse", action="store_true", required=False, dest="reparse", help="force re-creation of individual GTFS files (only valid with --merge)")
    argprs.add_argument("--maxfiles", action="store", required=False, dest="maxfiles", default=10, help="how many future files should be merged (only valid with --merge)")

    args = argprs.parse_args()
    print("""
    . . .                         ,---.--.--,---.,---.
    | | |,---.,---.,---.,---.. . .|  _.  |  |__. `---.
    | | |,---||    `---.,---|| | ||   |  |  |        |
    `-'-'`---^`    `---'`---^`-'-'`---'  `  `    `---'
    """)

    if type(args.maxfiles) is not int:
        try:
            args.maxfiles = int(args.maxfiles)
        except:
            raise ValueError("argument provided in --maxfiles has to be an integer ({} provided)".format(repr(args.maxfiles)))

    if args.merge:
        print("=== Creating a GTFS file for all future schedules ===")
        version = MultiDay.create(maxfiles=args.maxfiles, shapes=args.shapes, metro=args.metro, remerge=args.remerge, reparse=args.reparse)

    else:
        print("=== Creating a GTFS file for the current schedule ===")
        version = Converter.create(version=args.version, shapes=args.shapes, metro=args.metro, prevver=args.prevver)

    print("=== Finished making GTFS (ver {}) ===".format(version))
    print("Time elapsed: {:.3f} s".format(time.time() - st))
