package main

import (
	"flag"
	"log"
	"net/http"
	"path"
	"strings"

	"github.com/MKuranowski/WarsawGTFS/realtime/alerts"
	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
)

// Default CLI flags
var (
	flagTarget = flag.String(
		"t",
		"data_rt",
		"target folder where to put created GTFS-Realtime files")

	flagGtfs = flag.String(
		"gtfs-file",
		"https://mkuran.pl/gtfs/warsaw.zip",
		"path/URL to static Warsaw GTFS")

	flagJSON = flag.Bool(
		"json",
		false,
		"also save JSON files alongside GTFS-Relatime feeds")

	flagReadable = flag.Bool(
		"readable",
		false,
		"use a human-readable format for GTFS-Realtime target instead of a binary format")

	flagThrowDescErr = flag.Bool(
		"throw-desc-err",
		false,
		"if present, errors during retrieving/parsing alert body from wtp.waw.pl website will "+
			"be fatal instead of ignored")
)

// Other global objects
var client = &http.Client{}

// Main functionality
func main() {
	var err error

	// Parse CL flags
	flag.Parse()

	// Load gtfs
	log.Println("Loading provided GTFS")
	var gtfsFile *gtfs.Gtfs
	if strings.HasPrefix(*flagGtfs, "http://") || strings.HasPrefix(*flagGtfs, "https://") {
		gtfsFile, err = gtfs.NewGtfsFromURL(*flagGtfs, client)
	} else {
		gtfsFile, err = gtfs.NewGtfsFromFile(*flagGtfs)
	}

	if err != nil {
		log.Fatalln(err.Error())
	}

	// Load GTFS routes
	log.Println("Loading routes.txt")
	if routesFile := gtfsFile.GetZipFileByName("routes.txt"); routesFile != nil {
		gtfsFile.LoadRoutes(routesFile)
	} else {
		log.Fatalln("no file routes.txt in the GTFS")
	}

	// Create options struct
	alertOpts := alerts.Options{
		GtfsRtTarget:    path.Join(*flagTarget, "alerts.pb"),
		HumanReadable:   *flagReadable,
		ThrowLinkErrors: *flagThrowDescErr,
	}
	if *flagJSON {
		alertOpts.JSONTarget = path.Join(*flagTarget, "alerts.json")
	}

	// Make alerts
	log.Println("Creating Alerts feed")
	err = alerts.Make(client, gtfsFile.Routes, alertOpts)
	if err != nil {
		log.Fatalln(err.Error())
	}
}
