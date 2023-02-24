package positions

// cSpell: words cenkalti

import (
	"io"
	"log"
	"net/http"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
	"github.com/cenkalti/backoff/v4"
)

// Options represent options for creating positions GTFS-Realtime
type Options struct {
	GtfsRtTarget  string
	JSONTarget    string
	HumanReadable bool
	Apikey        string
	Brigades      string
}

// Create auto-magically creates realtime feeds with position data.
// Create is designed to run a loop, hence it doesn't contain logic to load
// brigades JSON
func Create(api VehicleAPI, brigadeMap map[string][]*brigadeEntry, prevVehicles map[string]*Vehicle, opts Options) (map[string]*Vehicle, error) {
	// 0. Prepare a container
	container := &VehicleContainer{}
	container.SyncTime = time.Now()
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
	if opts.JSONTarget != "" {
		err = container.SavePB(opts.GtfsRtTarget, opts.HumanReadable)
	}

	return container.Vehicles, err
}

// Main auto-magically creates vehicle position data
func Main(client *http.Client, opts Options) (err error) {
	// Load brigades.json
	brigadeMap, err := loadBrigades(opts.Brigades, client)
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

// Update automatically updates the RouteMap if the Resource has changed
func (rr *brigadesResource) Update() error {
	// Check for GTFS updates
	shouldUpdate, err := rr.Resource.Check()
	if err != nil {
		return err
	} else if shouldUpdate || rr.BrigadeMap == nil {
		log.Println("brigades.json changed, reloading")

		// Try to fetch updated brigades.json
		newData, err := rr.Resource.Fetch()
		if err != nil {
			return err
		}
		defer newData.Close()

		// Read brigades.json
		rawData, err := io.ReadAll(newData)
		if err != nil {
			return err
		}

		// Re-load brigades
		rr.BrigadeMap, err = makeBrigades(rawData)
		if err != nil {
			return err
		}
	}
	return nil
}

// Loop automatically updates the GTFS-RT Positions files
func Loop(client *http.Client, jsonResource util.Resource, sleepTime time.Duration, opts Options) (err error) {
	// Automatic wrapper around the resource
	var prevPositions map[string]*Vehicle
	api := VehicleAPI{Key: opts.Apikey, Client: client}
	br := brigadesResource{Resource: jsonResource}

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
