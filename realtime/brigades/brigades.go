package brigades

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// tripData is a struct for holding information about a matched trip
type tripData struct {
	TripID            string     `json:"trip_id"`
	TripTimeKey       string     `json:"-"`
	RouteID           string     `json:"-"`
	ServiceID         string     `json:"-"`
	BrigadeID         string     `json:"-"`
	LastStopIndex     uint64     `json:"-"`
	LastStopID        string     `json:"last_stop_id"`
	LastStopPos       [2]float64 `json:"last_stop_latlon"`
	LastStopTimepoint string     `json:"last_stop_timepoint"`
}

// MatchedTripData is a mapping from trip_id→tripData that represents trips matched with brigades
type MatchedTripData map[string]*tripData

// MarshalJSON marshals a MatchedTripData into JSON. This JSON has a different structure then
// MatchedTripData - nested objects will be used:
//   {
//     "{ROUTE_ID}": {
//       "{BRIGADE_ID}": [
//         {
//            "trip_id": "{TRIP_ID}",
//            "last_stop_id": "{STOP_ID}",
//            "last_stop_pos": [lat, lon], // an array of 2 floats
//            "last_stop_timepoint": "HH:MM:SS"
//         }
//       ]
//   }
func (m MatchedTripData) MarshalJSON() ([]byte, error) {
	// Distribute matched entries into a route_id → brigade_id → tripData map
	jsonMap := make(map[string]map[string][]*tripData)

	for _, t := range m {
		// Make map for route of this entry if it doens't exist
		if _, routeMapExists := jsonMap[t.RouteID]; !routeMapExists {
			jsonMap[t.RouteID] = make(map[string][]*tripData)
		}

		// Extract the list of this brigade's trips
		tripList := jsonMap[t.RouteID][t.BrigadeID]
		tripListLen := len(tripList)

		// Insert while keeping the list sorted by trip times
		insertIdx := sort.Search(
			tripListLen, func(i int) bool { return tripList[i].TripTimeKey >= t.TripTimeKey })

		if insertIdx < tripListLen {
			// If insertIdx falls inside the tripList, first shift the list to make room
			// for the new element, then save 't'
			tripList = append(tripList, nil)
			copy(tripList[insertIdx+1:], tripList[insertIdx:])
			tripList[insertIdx] = t
		} else {
			// If inserIdx is at the end of tripList, simply append this trip
			tripList = append(tripList, t)
		}

		// Set back tripList to the main map
		jsonMap[t.RouteID][t.BrigadeID] = tripList
	}

	// Marshall this map into JSON
	return json.MarshalIndent(jsonMap, "", "  ")
}

// stopTimeEvent is used for holding information from one stop_times.txt row
type stopTimeEvent struct {
	TripID    string
	RouteID   string
	ServiceID string
	StopID    string
	Index     uint64
	Time      string
}

// newStopTimeEvent creates a stopTimeEvent instance from a stop_times.txt row
func newStopTimeEvent(gtfs *gtfs.Gtfs, row map[string]string) (ste stopTimeEvent, err error) {
	// Ensure all required columns are defined
	err = util.MissingColumnCheck(
		"stop_times.txt",
		[]string{"trip_id", "stop_id", "stop_sequence", "departure_time"},
		row)
	if err != nil {
		return
	}

	// Extract required columns
	ste.TripID = row["trip_id"]
	ste.StopID = row["stop_id"]
	ste.Time = row["departure_time"]
	ste.Index, err = strconv.ParseUint(row["stop_sequence"], 10, 64)

	if err != nil {
		return
	}

	// Ensure the trip was defined earlier
	routeService, validTrip := gtfs.Trips[ste.TripID]
	if !validTrip {
		err = util.InvalidGtfsReference{
			ReferingFile:       "stop_times.txt",
			Column:             "trip_id",
			Value:              ste.TripID,
			ExpectedDefinition: "trips.txt",
		}
	}

	ste.RouteID = routeService.Route
	ste.ServiceID = routeService.Service

	return
}

// Match matches trips to brigade ids
func Match(api *ttableAPI, gtfs *gtfs.Gtfs, stopTimesReader io.Reader) (matches MatchedTripData, err error) {
	// Prepare the map for holding data
	matches = make(MatchedTripData)

	// Read stop_times.txt
	csvReader := csv.NewReader(stopTimesReader)
	header, err := csvReader.Read()

	if err != nil {
		return
	}

	for {
		// Try to get next row
		var rowSlice []string
		rowSlice, err = csvReader.Read()

		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return
		}

		// Convert row to map and assert all required columns are there
		var row stopTimeEvent
		rowMap := util.ZipStrings(header, rowSlice)
		row, err = newStopTimeEvent(gtfs, rowMap)
		if err != nil {
			return
		}

		// Check if this trip is bus/tram and if it's active on gtfs.SyncTime
		isActive := gtfs.Services[row.ServiceID]
		validRoute := util.StringSliceHas(gtfs.Routes["0"], row.RouteID) || util.StringSliceHas(gtfs.Routes["3"], row.RouteID)
		if !isActive || !validRoute {
			continue
		}

		// Fetch an entry for this trip from matches
		tripEntry, wasInserted := matches[row.TripID]

		// Create the entry if it didn't exist
		if !wasInserted {
			tripIDParts := strings.Split(row.TripID, "/")

			tripEntry = &tripData{
				TripID:      row.TripID,
				TripTimeKey: tripIDParts[len(tripIDParts)-1],
				RouteID:     row.RouteID,
				ServiceID:   row.ServiceID,
			}
			matches[row.TripID] = tripEntry
		}

		// Overwrite trip last stop
		if tripEntry.LastStopIndex <= row.Index {
			tripEntry.LastStopIndex = row.Index
			tripEntry.LastStopID = row.StopID
			tripEntry.LastStopTimepoint = row.Time
		}

		// If brigade was already matched, skip to next iteration
		if tripEntry.BrigadeID != "" {
			continue
		}

		// Get time→brigade mapping for this route-stop pair
		var mtb mapTimeBrigade
		var apiFromCache bool
		mtb, apiFromCache, err = api.Get(routeStopPair{Route: row.RouteID, Stop: row.StopID})
		if err != nil {
			return
		}

		// Find the brigadeId for this stop
		brigadeID, ok := mtb[row.Time]
		if !ok {
			logPrintf(
				"StopTimeEvent: T %s | R %s | S %s (from api: %t) ❌ NO MATCH FOR %s",
				false,
				row.TripID,
				row.RouteID,
				row.StopID,
				!apiFromCache,
				row.Time,
			)
			continue
		} else {
			tripEntry.BrigadeID = brigadeID
			// logPrintf(
			// 	"StopTimeEvent: T %s | R %s | S %s (from api: %t) ✔️ match for %s",
			// 	true,
			// 	row.TripID,
			// 	row.RouteID,
			// 	row.StopID,
			// 	!apiFromCache,
			// 	row.Time,
			// )
		}
	}

	// Remove unmatched trips and append LastStopPos
	for tripID, tripEntry := range matches {
		if tripEntry.BrigadeID == "" {
			delete(matches, tripID)
		} else {
			tripEntry.LastStopPos = gtfs.Stops[tripEntry.LastStopID]
		}
	}

	return
}
