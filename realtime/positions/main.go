package positions

// cSpell: words cenkalti

import (
	"log"
	"net/http"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
	"github.com/MKuranowski/WarsawGTFS/realtime/util"
	"github.com/cenkalti/backoff/v4"
)

// Options represent options for creating positions GTFS-Realtime
type Options struct {
	GtfsRtTarget  string
	JSONTarget    string
	HumanReadable bool
	Apikey        string
}

// Create auto-magically creates realtime feeds with position data.
// Create is designed to run a loop, hence it doesn't contain logic to load
// brigades JSON
func Create(api VehicleAPI, brigadeMap map[string][]*brigadeEntry, prevVehicles map[string]*Vehicle, opts Options) (map[string]*Vehicle, error) {
	// 0. Prepare a container
	container := &VehicleContainer{}
	container.SyncTime = time.Now().In(util.WarsawTimezone)
	container.CompareSyncTime = newCompareTimeFromTime(container.SyncTime)

	// 1. Get data from the api
	apiEntries, err := api.GetAll()
	if err != nil {
		return nil, err
	}

	// 2. Create Vehicle objects
	err = container.Prepare(apiEntries)
	if err != nil {
		return nil, err
	}

	// 3. Match all Vehicles to trips
	err = container.MatchAll(brigadeMap, prevVehicles)
	if err != nil {
		return nil, err
	}

	// 4. Export to JSON
	if opts.JSONTarget != "" {
		err = container.SaveJSON(opts.JSONTarget)
		if err != nil {
			return container.Vehicles, err
		}
	}

	// 5. Export to GTFS-RT
	if opts.GtfsRtTarget != "" {
		err = container.SavePB(opts.GtfsRtTarget, opts.HumanReadable)
	}

	return container.Vehicles, err
}

// Main auto-magically creates vehicle position data
func Main(client *http.Client, gtfsFile *gtfs.Gtfs, opts Options) (err error) {
	// Load brigades from gtfs
	brigadeMap, err := loadBrigades(gtfsFile)
	if err != nil {
		return
	}

	// Create other objects required by the Create function
	var prevVehicles map[string]*Vehicle
	api := VehicleAPI{Key: opts.Apikey, Client: client}

	// Call Create
	_, err = Create(api, brigadeMap, prevVehicles, opts)
	return
}

// routesResource is a pair of resource pointing to a GTFS file and a routeMap
type brigadesResource struct {
	Resource   util.Resource
	BrigadeMap map[string][]*brigadeEntry
}

func (rr *brigadesResource) ShouldUpdate() (bool, error) {
	// Force a refresh at 3:00, when a new service date begins operating
	gotDate := util.ServiceDate(rr.Resource.LastCheck().In(util.WarsawTimezone))
	expectedDate := util.ServiceDate(time.Now().In(util.WarsawTimezone))
	if expectedDate != gotDate {
		return true, nil
	}

	// Check if the GTFS has changed
	return rr.Resource.Check()
}

// Update automatically updates the RouteMap if the Resource has changed
func (rr *brigadesResource) Update() error {
	// Check for GTFS updates
	shouldUpdate, err := rr.ShouldUpdate()
	if err != nil {
		return err
	} else if shouldUpdate || rr.BrigadeMap == nil {
		log.Println("gtfs changed, reloading")

		// Fetch the new GTFS
		gtfsContent, err := rr.Resource.Fetch()
		if err != nil {
			return err
		}
		defer gtfsContent.Close()

		// Load the new GTFS
		gtfsFile, err := gtfs.NewGtfsFromReader(gtfsContent)
		if err != nil {
			return err
		}
		defer gtfsFile.Close()
		err = gtfsFile.LoadAll()
		if err != nil {
			return err
		}

		// Re-load brigades
		rr.BrigadeMap, err = loadBrigades(gtfsFile)
		if err != nil {
			return err
		}
	}
	return nil
}

// Loop automatically updates the GTFS-RT Positions files
func Loop(client *http.Client, gtfsResource util.Resource, sleepTime time.Duration, opts Options) (err error) {
	// Automatic wrapper around the resource
	var prevPositions map[string]*Vehicle
	api := VehicleAPI{Key: opts.Apikey, Client: client}
	br := brigadesResource{Resource: gtfsResource}

	// Backoff shit
	backoff := &backoff.ExponentialBackOff{
		InitialInterval:     10 * time.Second,
		RandomizationFactor: 0.3,
		Multiplier:          2,
		MaxInterval:         48 * time.Hour,
		MaxElapsedTime:      48 * time.Hour,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}

	for {
		// Try to update brigades.json
		err = br.Update()
		if err != nil {
			return
		}

		// Try updating the GTFS-RT
		backoff.Reset()
		for sleep := time.Duration(0); sleep != backoff.Stop; sleep = backoff.NextBackOff() {
			// Print error when backing off
			if sleep != 0 {
				// Log when backingoff
				sleepUntil := time.Now().Add(sleep).Format("15:04:05")
				log.Printf(
					"Updating the GTFS-RT Positions failed. Backoff until %s. Error: %q.\n",
					sleepUntil, err.Error(),
				)

				// Sleep for the backoff
				time.Sleep(sleep)
			}

			// Try to update the GTFS-RT
			var newPositions map[string]*Vehicle
			newPositions, err = Create(api, br.BrigadeMap, prevPositions, opts)

			// If no errors were encountered, break out of the backoff loop
			if err == nil {
				prevPositions, newPositions = newPositions, nil
				log.Println("GTFS-RT Positions updated successfully.")
				break
			}
		}
		if err != nil {
			return
		}

		// Sleep until next try
		time.Sleep(sleepTime)
	}

}
