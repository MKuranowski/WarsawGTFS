from argparse import ArgumentParser
from datetime import datetime
from time import time

from static import make_single, setup_logging

ASCII_ART = """
. . .                         ,---.--.--,---.,---.
| | |,---.,---.,---.,---.. . .|  _.  |  |__. `---.
| | |,---||    `---.,---|| | ||   |  |  |        |
`-'-'`---^`    `---'`---^`-'-'`---'  `  `    `---'
"""

if __name__ == "__main__":
    st = time()
    argprs = ArgumentParser()

    argprs.add_argument(
        "-m",
        "--metro",
        action="store_true",
        help="wheteher to add metro schedules from an external source"
    )

    argprs.add_argument(
        "-d",
        "--date",
        default=None,
        required=False,
        metavar="YYYYMMDD",
        help="date to determine which file should be parsed (only in single-file-mode)"
    )

    argprs.add_argument(
        "-t",
        "--target",
        default="gtfs.zip",
        required=False,
        metavar="gtfs.zip",
        help="where to put the created gtfs file"
    )

    argprs.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable more verbose logging"
    )

    argprs.add_argument(
        "-pn",
        "--publisher-name",
        default="",
        required=False,
        metavar="NAME",
        help="value of feed_publisher_name (--publisher-url is also required to create feed_info)"
    )

    argprs.add_argument(
        "-pu",
        "--publisher-url",
        default="",
        required=False,
        metavar="URL",
        help="value of feed_publisher_url (--publisher-name is also required to create feed_info)"
    )

    args = argprs.parse_args()
    print(ASCII_ART)

    # if args.merge:
    #     print("=== Creating a GTFS file for all future schedules ===")
    #     version = MultiDay.create(maxfiles=args.maxfiles, shapes=args.shapes, metro=args.metro,
    #                               remerge=args.remerge, reparse=args.reparse,
    #                               pub_name=args.publisher_name, pub_url=args.publisher_url)

    # Parse the 'date' argument
    if args.date:
        args.date = datetime.strptime(args.date, "%Y%m%d").date()

    # Prepare the logger
    setup_logging(args.verbose)

    print("=== Creating a GTFS file for the current schedule ===")
    make_single(args.date, args.target, args.publisher_name, args.publisher_url, args.metro)

    print(f"Time elapsed: {time() - st:.3f} s")
