package positions

import (
	"net/http"
	"time"
)

// CalculateLastStopTime sets the LastStopTime object on a brigadeEntry
func (b brigadeEntry) SetLastStopTime() (err error) {
	b.LastStopTime, err = newCompareTimeFromGtfs(b.LastStopTimepoint)
	return
}

// Options represent options for creating positions GTFS-Realtime
type Options struct {
	GtfsRtTarget  string
	JSONTarget    string
	HumanReadable bool
	Apikey        string
	Brigades      string
}

// Create auto-magically creates realtime feeds with position data.
// Create is designed to run a loop, hence it doesn't conatin logic to load
// brigades JSON and assumes
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
