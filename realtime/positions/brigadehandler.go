package positions

import (
	"fmt"
	"slices"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// brigadeEntry is an object representing a single run of a trip on a particular service date
type brigadeEntry struct {
	TripID            string
	ServiceDate       string
	LastStopID        string
	LastStopPos       [2]float64
	LastStopTimepoint string
	LastStopTime      time.Time
}

// loadBrigades creates a map from "V/route_id/brigade_id" to a list of brigadeEntry
// based on GTFS data. Trips from every date in gtfsFile.Services are included,
// so that runs from the previous service date past midnight (e.g. 27:15:00) are still present.
func loadBrigades(gtfsFile *gtfs.Gtfs) (m map[string][]*brigadeEntry, err error) {
	m = make(map[string][]*brigadeEntry)

	for id, data := range gtfsFile.Trips {
		// Ignore inactive trips
		if data.LastStopTime.Timepoint == "" {
			continue
		}

		key := fmt.Sprintf("V/%s/%s", data.Route, data.Brigade)

		for _, serviceDate := range gtfsFile.Services[data.Service] {
			lastStopTime, err := util.GtfsTime(serviceDate, data.LastStopTime.Timepoint)
			if err != nil {
				return m, fmt.Errorf("invalid last stop timepoint: %q: %w", data.LastStopTime.Timepoint, err)
			}

			entry := brigadeEntry{
				TripID:            id,
				ServiceDate:       serviceDate,
				LastStopID:        data.LastStopTime.StopID,
				LastStopPos:       gtfsFile.Stops[data.LastStopTime.StopID],
				LastStopTimepoint: data.LastStopTime.Timepoint,
				LastStopTime:      lastStopTime,
			}

			m[key] = append(m[key], &entry)
		}
	}

	for _, brigades := range m {
		slices.SortFunc(brigades, func(a, b *brigadeEntry) int {
			return a.LastStopTime.Compare(b.LastStopTime)
		})
	}

	return
}
