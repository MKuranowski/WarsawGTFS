package positions

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// brigadeEntry is an object represeting an object from brigades.json
type brigadeEntry struct {
	TripID            string      `json:"trip_id"`
	LastStopID        string      `json:"last_stop_id"`
	LastStopPos       [2]float64  `json:"last_stop_latlon"`
	LastStopTimepoint string      `json:"last_stop_timepoint"`
	LastStopTime      compareTime `json:"-"`
}

// readURL gets given url and returns the content under that url
func readURL(client *http.Client, url string) (buff []byte, err error) {
	// Make the request
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		err = util.RequestError{URL: url, Status: resp.Status, StatusCode: resp.StatusCode}
		return
	}

	// Read the content
	buff, err = ioutil.ReadAll(resp.Body)
	return
}

// makeBrigades take a JSON file (as a []byte) and tries to create a map
// from "V/route_id/brigade_id" to list of brigadeEntry
func makeBrigades(raw []byte) (m map[string][]*brigadeEntry, err error) {
	// Decode the JSON response
	var dataJSON map[string]map[string][]*brigadeEntry
	err = json.Unmarshal(raw, &dataJSON)

	// Unload data from JSON
	m = make(map[string][]*brigadeEntry)
	for routeID, brigadeMaps := range dataJSON {
		for brigadeID, brigadeEntries := range brigadeMaps {
			// Create an entry in brigadeEntries
			key := "V/" + routeID + "/" + brigadeID
			m[key] = brigadeEntries

			// Load the LastStopTime for every brigadeEntry
			for _, be := range brigadeEntries {
				be.LastStopTime, err = newCompareTimeFromGtfs(be.LastStopTimepoint)
				if err != nil {
					return
				}
			}
		}
	}
	return
}

// loadBrigades creates a map from "V/route_id/brigade_id" to a list of brigadeEntry
// from brigades.json file loaded from either a file or a http/https remote location
func loadBrigades(source string, client *http.Client) (m map[string][]*brigadeEntry, err error) {
	// Load data from the source
	var dataRaw []byte

	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		dataRaw, err = readURL(client, source)
	} else {
		dataRaw, err = ioutil.ReadFile(source)
	}

	if err != nil {
		return
	}

	return makeBrigades(dataRaw)
}
