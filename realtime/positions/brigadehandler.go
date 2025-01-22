package positions

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
)

// brigadeEntry is an object representing an object from brigades.json
type brigadeEntry struct {
	TripID            string
	LastStopID        string
	LastStopPos       [2]float64
	LastStopTimepoint string
	LastStopTime      compareTime
}

// loadBrigades creates a map from "V/route_id/brigade_id" to a list of brigadeEntry
// based on GTFS data.
func loadBrigades(gtfsFile *gtfs.Gtfs) (m map[string][]*brigadeEntry, err error) {
	m = make(map[string][]*brigadeEntry)

	for id, data := range gtfsFile.Trips {
		// Ignore inactive trips
		if !gtfsFile.Services[data.Service] || data.LastStopTime.Timepoint == "" {
			continue
		}

		lastStopTime, err := newCompareTimeFromGtfs(data.LastStopTime.Timepoint)
		if err != nil {
			return m, fmt.Errorf("invalid last stop timepoint: %q: %w", data.LastStopTime.Timepoint, err)
		}

		key := fmt.Sprintf("V/%s/%s", data.Route, data.Brigade)
		entry := brigadeEntry{
			TripID:            id,
			LastStopID:        data.LastStopTime.StopID,
			LastStopPos:       gtfsFile.Stops[data.LastStopTime.StopID],
			LastStopTimepoint: data.LastStopTime.Timepoint,
			LastStopTime:      lastStopTime,
		}

		m[key] = append(m[key], &entry)
	}

	for _, brigades := range m {
		slices.SortFunc(brigades, func(a, b *brigadeEntry) int {
			return cmp.Compare(a.LastStopTimepoint, b.LastStopTimepoint)
		})
	}

	return
}
