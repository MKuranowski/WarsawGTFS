package positions

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/golang/protobuf/proto"
)

// Vehicle is an object for representing a single vehicle position
type Vehicle struct {
	// Basic fields
	ID         string  `json:"id"`
	Time       string  `json:"timestamp"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	SideNumber string  `json:"side_number"`

	// Fields not filled by NewVehicle method
	Trip    string  `json:"trip_id"`
	Bearing float64 `json:"bearing,omitempty"`

	// Private fields, not meant to be exported
	Line    string    `json:"-"`
	Brigade string    `json:"-"`
	TimeObj time.Time `json:"-"`
}

// NewVehicle creates a Vehicle object from an apiVehicle object
func NewVehicle(av *APIVehicleEntry) (v *Vehicle, err error) {
	// Fill basic fields
	v = &Vehicle{
		ID:         "V/" + av.Lines + "/" + av.Brigade,
		Lat:        av.Lat,
		Lon:        av.Lon,
		SideNumber: av.VehicleNumber,
		Line:       av.Lines,
		Brigade:    av.Brigade,
	}

	// Try to parse the time
	v.TimeObj, err = time.Parse("2006-01-02 15:04:05", av.Time)
	v.Time = v.TimeObj.Format("2006-01-02T15:04:05")
	return
}

// AsProto returns the Vehicle marshalled into a gtsrt.FeedEntity
func (v *Vehicle) AsProto() *gtfsrt.FeedEntity {
	lat32 := float32(v.Lat)
	lon32 := float32(v.Lon)
	bearing32 := float32(v.Bearing)
	tstamp := uint64(v.TimeObj.Unix())

	return &gtfsrt.FeedEntity{
		Id: &v.ID,
		Vehicle: &gtfsrt.VehiclePosition{
			Trip:    &gtfsrt.TripDescriptor{TripId: &v.Trip},
			Vehicle: &gtfsrt.VehicleDescriptor{Id: &v.ID, Label: &v.SideNumber},
			Position: &gtfsrt.Position{
				Latitude:  &lat32,
				Longitude: &lon32,
				Bearing:   &bearing32,
			},
			Timestamp: &tstamp,
		},
	}
}

// CalculateBearing updates the bearing for a Vehicle
func (v *Vehicle) CalculateBearing(pv *Vehicle) {
	if pv == nil {
		// Bearing can't be calculated if the previous vehicle is unknown
		return
	} else if dist := haversine(pv.Lat, pv.Lon, v.Lat, v.Lon); dist < 0.02 {
		// If the vehicle hasn't moved 20 meters, re-write the previous bearing
		v.Bearing = pv.Bearing
	} else {
		// Otherwise, calculate the new bearing
		v.Bearing = initialBearing(pv.Lat, pv.Lon, v.Lat, v.Lon)
	}
}

// MatchTripNoPV tries to guess which trip this vehicle is on, without knowing
// the previous trip
func (v *Vehicle) MatchTripNoPV(cst compareTime, be []*brigadeEntry) error {
	// find first trip, which ends after the synctime -
	// that is the current trip, assuming the vehicle runs on schedule
	for _, b := range be {
		endsInFuture, err := b.LastStopTime.After(cst)
		if err != nil {
			return err
		} else if endsInFuture {
			v.Trip = b.TripID
			break
		}
	}

	// if the synctime is past the last trip end time, assume the vehicle is still doing the
	// last trip
	if v.Trip == "" {
		v.Trip = be[len(be)-1].TripID
	}

	return nil
}

// MatchTripWithPV tries to guess which trip this vehicle is on
func (v *Vehicle) MatchTripWithPV(pv *Vehicle, cst compareTime, be []*brigadeEntry) error {
	// first - get the index in be of pv.Trip
	prevTripIdx := indexMatchingTrip(be, pv.Trip)

	// handle some edge cases
	if prevTripIdx < 0 {
		// previous trip not found in be (brigades changed?) - calculate as if
		// the previous trip was not known
		return v.MatchTripNoPV(cst, be)
	} else if prevTripIdx == len(be)-1 {
		// vehicle was doing the last trip -there's nothing to calculate
		v.Trip = pv.Trip
		return nil
	}

	// extract info about previous vehicle trip
	pvTripEntry := be[prevTripIdx]
	pvTripTerminiLat := pvTripEntry.LastStopPos[0]
	pvTripTerminiLon := pvTripEntry.LastStopPos[1]

	secondsToEnd, err := pvTripEntry.LastStopTime.Since(cst)
	if err != nil {
		return err
	}

	// The vehicle is "nearTerminus" if it's 50 meters to tripLastStop
	nearTerminus := haversine(v.Lat, v.Lon, pvTripTerminiLat, pvTripTerminiLon) <= 0.05

	// The vehicle is "nearEndTime" if it's 4 minutes to or past the tripLastTime
	nearEndTime := secondsToEnd < 240

	// This is a fail-safe assumption that no trip is delayed more then 30 minutes
	shouldveFinished := secondsToEnd < -1800

	// Move to the next trip if it's close to the trip end (in time and space) or
	// if the fail-safe is active
	if (nearTerminus && nearEndTime) || shouldveFinished {
		v.Trip = be[prevTripIdx+1].TripID
	} else {
		v.Trip = pv.Trip
	}
	return nil
}

// MatchTrip tries to guess which trip this vehicle is on
func (v *Vehicle) MatchTrip(pv *Vehicle, cst compareTime, be []*brigadeEntry) error {
	// Handle some edge cases
	if len(be) == 0 {
		// No corresponding brigade entries - an 'inactive vehicle' -
		// don't set the v.Trip attribute (to mark this vehicle as ignorable)
		// and don't return any error states.
		return nil
	} else if pv == nil {
		// The previous entry for this vehicle id is unknown
		return v.MatchTripNoPV(cst, be)
	}

	return v.MatchTripWithPV(pv, cst, be)
}

// VehicleContainer is a container for multiple Vehicle objects
type VehicleContainer struct {
	SyncTime        time.Time
	CompareSyncTime compareTime
	Vehicles        map[string]*Vehicle
}

// MarshalJSON returns this VehicleContainer marshalled into JSON
func (vc *VehicleContainer) MarshalJSON() ([]byte, error) {
	// Create a list of vehicles
	vehList := make([]*Vehicle, 0, len(vc.Vehicles))
	for _, veh := range vc.Vehicles {
		vehList = append(vehList, veh)
	}

	return json.MarshalIndent(
		struct {
			Time      string     `json:"time"`
			Positions []*Vehicle `json:"positions"`
		}{
			Time:      vc.SyncTime.Format("2006-02-01 15:04:05"),
			Positions: vehList,
		},
		"",
		"  ",
	)
}

// SaveJSON marshalls the container into a json file at a given location
func (vc *VehicleContainer) SaveJSON(target string) (err error) {
	buff, err := vc.MarshalJSON()
	if err != nil {
		return
	}

	err = ioutil.WriteFile(target, buff, 0o666)
	return
}

// AsProto returns this VehicleContainer marshalled into a GTFS-RT FeedMessage
func (vc *VehicleContainer) AsProto() *gtfsrt.FeedMessage {
	msg := util.MakeFeedMessage(vc.SyncTime)
	msg.Entity = make([]*gtfsrt.FeedEntity, 0, len(vc.Vehicles))
	for _, veh := range vc.Vehicles {
		msg.Entity = append(msg.Entity, veh.AsProto())
	}
	return msg
}

// SavePB marshalls the container into a GTFS-Realtime protocol buffer file
func (vc *VehicleContainer) SavePB(target string, humanReadable bool) (err error) {
	// Open target file
	f, err := os.Create(target)
	if err != nil {
		return
	}
	defer f.Close()

	// Marshall to GTFS-RT
	if humanReadable {
		// Human-readable format
		err = proto.MarshalText(f, vc.AsProto())
		if err != nil {
			return
		}
	} else {
		// Binary format
		var b []byte
		b, err = proto.Marshal(vc.AsProto())
		if err != nil {
			return
		}
		f.Write(b)
	}

	return
}

// Prepare initializes the vehiclecontainer.Vehicles map with
// vehicle objects created from a sequence of apiVehicleEntry
func (vc *VehicleContainer) Prepare(apiEntries []*APIVehicleEntry) error {
	vc.Vehicles = make(map[string]*Vehicle, len(apiEntries))

	for _, ae := range apiEntries {
		v, err := NewVehicle(ae)
		if err != nil {
			return err
		}
		vc.Vehicles[v.ID] = v
	}

	return nil
}

// MatchAll tries to calculate the bearing and to match a vehicle to a prticular trip
// for all its Vehicles. If a Vehicle still has an empty Trip field after calling its Match
// function such vehicle is removed for the container.
func (vc *VehicleContainer) MatchAll(brigadeMap map[string][]*brigadeEntry, prevVehicles map[string]*Vehicle) error {
	for vID, v := range vc.Vehicles {
		// Try to find matching brigade fields
		be := brigadeMap[vID]

		// Try to find precious Vehicle for this ID
		pv := prevVehicles[vID]

		// Match this vehicle to a particular trip
		err := v.MatchTrip(pv, vc.CompareSyncTime, be)
		if err != nil {
			return err
		}

		// Remove this vehicle if no trip was matched
		if v.Trip == "" {
			delete(vc.Vehicles, vID)
			continue
		}

		// Caulcate the bearing
		v.CalculateBearing(pv)
	}

	return nil
}
