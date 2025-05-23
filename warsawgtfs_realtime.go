package main

import (
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/MKuranowski/WarsawGTFS/realtime/alerts"
	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
	"github.com/MKuranowski/WarsawGTFS/realtime/positions"
	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

/* ===============
   GLOBAL OBJECTS
   & CLI FLAGS
  ================ */

// Default http client
var client *http.Client = http.DefaultClient

// Default CLI flags
var (
	// Mode selection
	flagAlerts = flag.Bool(
		"a",
		false,
		"create GTFS-Realtime alerts")

	flagPositions = flag.Bool(
		"p",
		false,
		"create GTFS-Realtime vehicle positions")

	// Input options
	flagApikey = flag.String(
		"k",
		"",
		"apikey for api.um.warszawa.pl (for positions), if empty/omitted - use WARSAW_APIKEY env variable")

	flagGtfsFile = flag.String(
		"gtfs-file",
		"https://mkuran.pl/gtfs/warsaw.zip",
		"path/URL to static Warsaw GTFS (for alerts and positions)")

	// Output options
	flagTarget = flag.String(
		"target",
		"data_rt",
		"target folder where to put created GTFS-Realtime files")

	flagJSON = flag.Bool(
		"json",
		false,
		"also save JSON files alongside GTFS-Realtime feeds")

	flagReadable = flag.Bool(
		"readable",
		false,
		"use a human-readable format for GTFS-Realtime target instead of a binary format")

	flagStrict = flag.Bool(
		"strict",
		false,
		"for alerts: any errors when scraping wtp.waw.pl will become fatal,\n"+
			"for brigades: any ignorable data mismatch will become fatal")

	// Loop options
	flagLoop = flag.Duration(
		"loop",
		time.Duration(0),
		"instead of running once and exiting, "+
			"update the output file every given duration (alerts/positions only)")

	flagDataCheck = flag.Duration(
		"checkdata",
		30*time.Minute,
		"how often check if the -gtfs-file has changed")
)

/* ================
   FLAG PROCESSING
  ================= */

func getApikey() string {
	if *flagApikey == "" {
		return os.Getenv("WARSAW_APIKEY")
	}
	return *flagApikey
}

// checkModes ensures exactly one of flagAlerts, flagBrigades or flagPositions is set
func checkModes() error {
	var modeCount uint8

	if *flagAlerts {
		modeCount++
	}
	if *flagPositions {
		modeCount++
	}

	if modeCount != 1 {
		return errors.New("exactly one of the -a or -p flags has to be provided")
	}
	return nil
}

// parseAlertsFlags parses flags to alert.Options
func parseAlertsFlags() (o alerts.Options, err error) {
	o.GtfsRtTarget = path.Join(*flagTarget, "alerts.pb")
	o.HumanReadable = *flagReadable
	o.ThrowLinkErrors = *flagStrict

	if *flagJSON {
		o.JSONTarget = path.Join(*flagTarget, "alerts.json")
	}

	return
}

// parsePositionsFlags parses flags to positions.Options
func parsePositionsFlags() (o positions.Options, err error) {
	// Ensure an apikey was provided
	o.Apikey = getApikey()
	if o.Apikey == "" {
		err = errors.New("key for api.um.warszawa.pl needs to be provided")
		return
	}

	// Set options
	o.GtfsRtTarget = path.Join(*flagTarget, "positions.pb")
	o.HumanReadable = *flagReadable
	if *flagJSON {
		o.JSONTarget = path.Join(*flagTarget, "positions.json")
	}
	return
}

/* =================
   DATA PREPARATION
  ================== */

// loadGtfs creates a gtfs file from the provided argument and loads required data structures
func loadGtfs(routesOnly bool) (gtfsFile *gtfs.Gtfs, err error) {
	// retrieve the GTFS
	log.Println("Retrieving provided GTFS")
	if strings.HasPrefix(*flagGtfsFile, "http://") || strings.HasPrefix(*flagGtfsFile, "https://") {
		gtfsFile, err = gtfs.NewGtfsFromURL(*flagGtfsFile, client)
	} else {
		gtfsFile, err = gtfs.NewGtfsFromFile(*flagGtfsFile)
	}

	if err != nil {
		return
	}

	// Load data
	if routesOnly {
		log.Println("Loading routes.txt")
		if routesFile := gtfsFile.GetZipFileByName("routes.txt"); routesFile != nil {
			err = gtfsFile.LoadRoutes(routesFile)
		} else {
			err = errors.New("no file routes.txt in the GTFS")
		}
	} else {
		log.Println("Loading data from the GTFS")
		err = gtfsFile.LoadAll()
	}

	// Close gtfsFile if an error occurred
	if err != nil {
		gtfsFile.Close()
	}

	return
}

// wrapInResource wraps a "file" on local fs or on the internet inside a util.Resource
func wrapInResource(source string) (res util.Resource) {
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		res = &util.ResourceHTTP{
			Client: client, URL: source, Period: *flagDataCheck,
		}
	} else {
		res = &util.ResourceLocal{Path: source, Period: *flagDataCheck}
	}
	return
}

/* ===============
   LOOP OPERATION
  ================ */

// loopAlerts prepares options for launching alerts in a loop mode
// and then returns launches alerts.Loop
func loopAlerts() error {
	opts, err := parseAlertsFlags()
	if err != nil {
		return err
	}
	res := wrapInResource(*flagGtfsFile)
	return alerts.Loop(client, res, *flagLoop, opts)
}

// loopPositions prepares options for launching positions in a loop mode
// and then returns launches positions.Loop
func loopPositions() error {
	opts, err := parsePositionsFlags()
	if err != nil {
		return err
	}
	res := wrapInResource(*flagGtfsFile)
	return positions.Loop(client, res, *flagLoop, opts)
}

/* ============
   SINGLE-PASS
    OPERATION
  ============= */

// singleAlerts prepares options for creating alerts and then creates them
func singleAlerts() error {
	// Get options
	opts, err := parseAlertsFlags()
	if err != nil {
		return err
	}

	// Get GTFS route map
	gtfsFile, err := loadGtfs(true)
	if err != nil {
		return err
	}
	gtfsFile.Close()

	// Make alerts
	log.Println("Creating alerts")
	return alerts.Make(client, gtfsFile.Routes, opts)
}

// singlePositions parses options for creating positions and then creates them
func singlePositions() error {
	// Get options
	opts, err := parsePositionsFlags()
	if err != nil {
		return err
	}

	// Get GTFS
	gtfsFile, err := loadGtfs(false)
	if err != nil {
		return err
	}
	gtfsFile.Close()

	// Make positions
	log.Println("Creating positions")
	return positions.Main(client, gtfsFile, opts)
}

/* ============
   ENTRY POINT
  ============= */

// Main functionality
func main() {
	var err error

	// Parse CL flags
	flag.Parse()

	// Check excluding flags
	loopMode := *flagLoop > 0
	err = checkModes()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Select the appropriate function to call
	var modeFunc func() error
	switch {

	// loop mode enabled
	case *flagAlerts && loopMode:
		modeFunc = loopAlerts
	case *flagPositions && loopMode:
		modeFunc = loopPositions
	case loopMode:
		modeFunc = func() error { return errors.New("loop mode is available only for alerts/positions") }

	// single pass
	case *flagAlerts:
		modeFunc = singleAlerts
	case *flagPositions:
		modeFunc = singlePositions
	}

	// create the target directory
	err = os.Mkdir(*flagTarget, 0o777)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		log.Fatalf("mkdir %s: %v", *flagTarget, err)
	}

	// Execute the selected mode
	err = modeFunc()
	if err != nil {
		log.Fatalln(err.Error())
	}
}
