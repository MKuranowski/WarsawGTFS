from argparse import ArgumentParser
from datetime import datetime
from time import time

from static import ConversionOpts, make_multiple, make_single, setup_logging

# cSpell: words remerge

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
        help="add metro schedules from an external source")

    argprs.add_argument(
        "-s",
        "--shapes",
        action="store_true",
        help="generate shapes.txt for all trips")

    argprs.add_argument(
        "-ns",
        "--no-shape-simplification",
        action="store_true",
        help="disable shape simplification (only valid with -s)")

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

    # GTFS Route Color
    argprs.add_argument(
        "--bus-color",
        default="880077",
        type=str,
        help="color of bus route (default: #880077)")

    argprs.add_argument(
        "--tram-color",
        default="B60000",
        type=str,
        help="color of tram route (default: #B60000)")

    argprs.add_argument(
        "--night-bus-color",
        default="000000",
        type=str,
        help="color of night bus route (default: #000000)")

    argprs.add_argument(
        "--bus-express-color",
        default="B60000",
        type=str,
        help="color of tram route (default: #B60000)")

    argprs.add_argument(
        "--train-color",
        default="009955",
        type=str,
        help="color of train route (default: #009955)")

    argprs.add_argument(
        "--zone-color",
        default="006800",
        type=str,
        help="color of zone route (default: #006800)")

    argprs.add_argument(
        "--special-color",
        default="B60000",
        type=str,
        help="color of zone route (default: #B60000)")

    argprs.add_argument(
        "--supplementary-color",
        default="000088",
        type=str,
        help="color of zone route (default: #000088)")

    # GTFS Route Text Color
    argprs.add_argument(
        "--bus-text-color",
        default="FFFFFF",
        type=str,
        help="color of text bus route (default: #FFFFFF)")

    argprs.add_argument(
        "--tram-text-color",
        default="FFFFFF",
        help="color of text tram route (default: #FFFFFF)")

    argprs.add_argument(
        "--night-bus-text-color",
        default="FFFFFF",
        type=str,
        help="color of text night bus route (default: #FFFFFF)")

    argprs.add_argument(
        "--bus-express-text-color",
        default="FFFFFF",
        type=str,
        help="color of text tram route (default: #FFFFFF)")

    argprs.add_argument(
        "--train-text-color",
        default="FFFFFF",
        type=str,
        help="color of text train route (default: #FFFFFF)")

    argprs.add_argument(
        "--zone-text-color",
        default="FFFFFF",
        type=str,
        help="color of text zone route (default: #FFFFFF)")

    argprs.add_argument(
        "--special-text-color",
        default="FFFFFF",
        type=str,
        help="color of text zone route (default: #FFFFFF)")

    argprs.add_argument(
        "--supplementary-text-color",
        default="FFFFFF",
        type=str,
        help="color of text zone route (default: #FFFFFF)")

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
        shapes=args.shapes,
        simplify_shapes=not args.no_shape_simplification,

        bus_color=args.bus_color,
        tram_color=args.tram_color,
        bus_express_color=args.bus_express_color,
        night_bus_color=args.night_bus_color,
        train_color=args.train_color,
        zone_color=args.zone_color,
        special_color=args.special_color,
        supplementary_color=args.supplementary_color,

        bus_text_color=args.bus_text_color,
        tram_text_color=args.tram_text_color,
        bus_express_text_color=args.bus_express_text_color,
        night_bus_text_color=args.night_bus_text_color,
        train_text_color=args.train_text_color,
        zone_text_color=args.zone_text_color,
        special_text_color=args.special_text_color,
        supplementary_text_color=args.supplementary_text_color
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
