package brigades

import (
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
)

// Options represents options for creating breigades.json
type Options struct {
	JSONTarget     string
	Apikey         string
	ThrowAPIErrors bool
}

// Main auto-magically creates brigades data
func Main(client *http.Client, gtfs *gtfs.Gtfs, opts Options) error {
	// Create an API object
	api := &ttableAPI{
		Key:           opts.Apikey,
		Client:        client,
		Reposnses:     make(map[routeStopPair]mapTimeBrigade),
		ForwardErrors: opts.ThrowAPIErrors,
	}

	// Try to open stop_times.txt
	file := gtfs.GetZipFileByName("stop_times.txt")
	if file == nil {
		return errors.New("gtfs file is missing stop_times.txt`1")
	}
	reader, err := file.Open()
	if err != nil {
		return err
	}
	defer reader.Close()

	// Match data
	log.Println("Matching data")
	data, err := Match(api, gtfs, reader)
	if err != nil {
		return err
	}

	// Marshall it to JSON
	log.Println("Marshalling data to JSON")
	dataJSON, err := data.MarshalJSON()
	if err != nil {
		return err
	}

	// Create a target file
	log.Println("Writing to JSON")
	f, err := os.Create(opts.JSONTarget)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write JSON to the target file
	_, err = f.Write(dataJSON)
	return err
}
