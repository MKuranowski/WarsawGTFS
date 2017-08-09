def warsawgtfs(getDate="", prevVer="", local=False):
    from scripts import config, finish, get, parser

    print("Loading config")
    conf = config.load()
    if not conf:
        exit()

    #Directories cleanup
    get.cleanup(local)

    if local:
        print("Finding local file to parse")
        filename = get.findfile()

    else:
        print("Downloading ZTM file")
        filename = get.download(getDate, prevVer)

    if not filename:
        print("File already parsed, aborting")
        return(prevVer)

    print("Converting to GTFS")
    parser.parse(filename, conf)

    if conf["addMetro"]:
        print("Adding metro schedules")
        finish.addMetro

    print("Creating fare files")
    finish.fare()

    print("Generating feed_info and agency files")
    finish.agency(conf)
    finish.feedinfo(filename)

    print("Zipping to gtfs.zip")
    finish.zip()

    return filename.lstrip("input/").rstrip(".txt")

if __name__ == "__main__":
    import argparse
    from datetime import date
    import time
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-l", "--local", action="store_true", required=False, dest="local", help="parse first that matches input/RA*.txt format, instead of downloading the file")
    argprs.add_argument("-d", "--date", default="", required=False, metavar="yymmdd", dest="date", help="date for which schedules should be downloaded, if not today")
    argprs.add_argument("-p", "--prevver", default="", required=False, metavar="RAyymmdd", dest="prevver", help="previous feed_version, if you want to avoid downloading the same file again")
    args = vars(argprs.parse_args())
    print("""
    . . .                         ,---.--.--,---.,---.
    | | |,---.,---.,---.,---.. . .|  _.  |  |__. `---.
    | | |,---||    `---.,---|| | ||   |  |  |        |
    `-'-'`---^`    `---'`---^`-'-'`---'  `  `    `---'
    """)
    if args["local"]:
        print("Local file will be parsed")
    elif args["date"]:
        print("Schedules will be downloaded for %s" % args["date"])
    else:
        print("Schedules will be downloaded for today (%s)" % date.today().strftime("%y%m%d"))
    if args["prevver"]:
        print("If active schedules version matches %s, no new file will be created" % args["prevver"])
    version = warsawgtfs(args["date"], args["prevver"], args["local"])
    print("=== Done! ===")
    print("Parsed version: %s" % version)
    print("Time elapsed: %s s" % round(time.time() - st, 3))
