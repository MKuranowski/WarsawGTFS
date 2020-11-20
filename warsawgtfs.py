from argparse import ArgumentParser
from datetime import datetime
from time import time

from static import ConversionOpts, make_multiple, make_single, setup_logging

ASCII_ART = """
. . .                         ,---.--.--,---.,---.
| | |,---.,---.,---.,---.. . .|  _.  |  |__. `---.
| | |,---||    `---.,---|| | ||   |  |  |        |
`-'-'`---^`    `---'`---^`-'-'`---'  `  `    `---'
"""

if __name__ == "__main__":
    argprs = ArgumentParser()

    # DATA CONVERSION OPTIONS #

    argprs.add_argument(
        "-m",
        "--metro",
        action="store_true",
        help="wheteher to add metro schedules from an external source")

    argprs.add_argument(
        "-s",
        "--shapes",
        action="store_true",
        help="wheteher to generate shapes.txt for all trips")

    argprs.add_argument(
        "-d",
        "--date",
        default=None,
        metavar="YYYYMMDD",
        help="date to determine from which day created feed should be valid")

    argprs.add_argument(
        "-t",
        "--target",
        default="gtfs.zip",
        metavar="gtfs.zip",
        help="where to put the created gtfs file")

    # MULTI-DAY OPTIONS #

    argprs.add_argument(
        "-r",
        "--merge",
        action="store_true",
        help="automatically create and merge all future schedules")

    argprs.add_argument(
        "-rm",
        "--remerge",
        action="store_true",
        help="force merge of multi-day file (only valid with --merge)")

    argprs.add_argument(
        "-rp",
        "--reparse",
        action="store_true",
        help="force re-creation of individual GTFS files (only valid with --merge)")

    argprs.add_argument(
        "-rf",
        "--maxfiles",
        action="store",
        type=int,
        default=5,
        help="how many future files should be merged (only valid with --merge)")

    # WARSAWGTFS OPTIONS #

    argprs.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable more verbose logging")

    # GTFS META-DATA #

    argprs.add_argument(
        "-pn",
        "--publisher-name",
        default="",
        metavar="NAME",
        help="value of feed_publisher_name (--publisher-url is also required to create feed_info)")

    argprs.add_argument(
        "-pu",
        "--publisher-url",
        default="",
        metavar="URL",
        help="value of feed_publisher_url (--publisher-name is also required to create feed_info)")

    # Parse command line options and print an ascii_art text
    args = argprs.parse_args()
    print(ASCII_ART)

    # Parse the 'date' argument
    if args.date:
        args.date = datetime.strptime(args.date, "%Y%m%d").date()

    # Prepare the logger
    setup_logging(args.verbose)

    # Prepare ConversionOpts
    opts = ConversionOpts(
        target=args.target,
        sync_time="",
        pub_name=args.publisher_name,
        pub_url=args.publisher_url,
        metro=args.metro,
        shapes=args.shapes
    )

    # Create GTFS
    st = time()

    if args.merge:
        print("=== Creating a GTFS file for all future schedules ===")
        make_multiple(opts, args.date, args.maxfiles, args.reparse, args.remerge)

    else:
        print("=== Creating a GTFS file for the current schedule ===")
        make_single(opts, args.date)

    print(f"Time elapsed: {time() - st:.3f} s")
