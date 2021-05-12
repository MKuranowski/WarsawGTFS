package positions

import (
	"math"
)

// radians - converts from degrees to radians
func radians(x float64) float64 {
	return x * math.Pi / 180
}

// degrees - converts from radians to degrees
func degrees(x float64) float64 {
	return x * 180 / math.Pi
}

// haversine calculates the distance between 2 points in km
func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	// Convert to radians
	lat1 = radians(lat1)
	lon1 = radians(lon1)
	lat2 = radians(lat2)
	lon2 = radians(lon2)

	dlathalf := (lat2 - lat1) / 2
	dlonhalf := (lon2 - lon1) / 2

	a := math.Pow(math.Sin(dlathalf), 2)
	b := math.Pow(math.Sin(dlonhalf), 2)
	c := math.Sqrt(a + (b * math.Cos(lat1) * math.Cos(lat2)))

	return 2 * 6371 * math.Asin(c)
}

// initialBearing calculates the initial bearing from (lat1, lon1) to (lat2, lon2).
// Returned value should be in the <-180°, 180°> range.
func initialBearing(lat1, lon1, lat2, lon2 float64) float64 {
	// Convert to radians
	lat1 = radians(lat1)
	lat2 = radians(lat2)
	dlon := radians(lon2 - lon1)

	// Calculate atan2 arguments
	x := math.Sin(dlon) * math.Cos(lat2)
	y1 := math.Cos(lat1) * math.Sin(lat2)
	y2 := math.Sin(lat1) * math.Cos(lat2) * math.Cos(dlon)
	y := y1 - y2

	// Calculate the initial bearing, then return it in degrees
	bearing := math.Atan2(x, y)
	return degrees(bearing)
}

// indexMatchingTrip returns the index of first *brigadeEntry
// with the same trip as provided searchTrip.
// Returns -1 if no matches were found.
func indexMatchingTrip(s []*brigadeEntry, searchTrip string) int {
	for idx, elem := range s {
		if elem.TripID == searchTrip {
			return idx
		}
	}
	return -1
}
