package positions

import (
	"encoding/json"
	"os"
	"slices"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
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
	Trip        string  `json:"trip_id"`
	ServiceDate string  `json:"service_date"`
	Bearing     float64 `json:"bearing,omitempty"`

	// Private fields, not meant to be exported
	Line    string    `json:"-"`
	Brigade string    `json:"-"`
	TimeObj time.Time `json:"-"`
}

// NewVehicle creates a Vehicle object from an apiVehicle object
func NewVehicle(av *APIVehicleEntry, timezone *time.Location) (v *Vehicle, err error) {
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
	v.TimeObj, err = time.ParseInLocation("2006-01-02 15:04:05", av.Time, timezone)
	v.Time = v.TimeObj.Format(time.RFC3339)
	return
}

// AsProto returns the Vehicle marshalled into a gtfs.FeedEntity
func (v *Vehicle) AsProto() *gtfsrt.FeedEntity {
	lat32 := float32(v.Lat)
	lon32 := float32(v.Lon)
	bearing32 := float32(v.Bearing)
	tstamp := uint64(v.TimeObj.Unix())

	return &gtfsrt.FeedEntity{
		Id: &v.ID,
		Vehicle: &gtfsrt.VehiclePosition{
			Trip:    &gtfsrt.TripDescriptor{TripId: &v.Trip, StartDate: &v.ServiceDate},
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
func (v *Vehicle) MatchTripNoPV(st time.Time, be []*brigadeEntry) error {
	// find first trip, which ends after the synctime -
	// that is the current trip, assuming the vehicle runs on schedule
	idx := slices.IndexFunc(be, func(b *brigadeEntry) bool { return b.LastStopTime.After(st) })

	switch {
	case idx < 0:
		// The synctime is past the very last trip of the brigade -
		// assume the vehicle is doing that trip if and only if
		// it's no more then 15 minutes past its end
		if lastBE := be[len(be)-1]; lastBE.LastStopTime.Sub(st) >= -15*time.Minute {
			v.setTrip(lastBE)
		}

	case idx >= 1 && be[idx-1].ServiceDate != be[idx].ServiceDate:
		// The synctime is between the duties on two service dates.
		// Assume the vehicle is still doing the last trip of the previous duty
		// if it's no more then 15 minutes past its end.
		// Otherwise, assume the vehicle is doing the first trip of the next duty
		// if it ends within 2 hours - that is, the trip has (almost) started,
		// as no trip takes longer than ~1.5 hours.
		// If neither holds, the vehicle is not active on the network.
		if lastBE := be[idx-1]; lastBE.LastStopTime.Sub(st) >= -15*time.Minute {
			v.setTrip(lastBE)
		} else if be[idx].LastStopTime.Sub(st) <= 2*time.Hour {
			v.setTrip(be[idx])
		}

	default:
		v.setTrip(be[idx])
	}
	return nil
}

// MatchTripWithPV tries to guess which trip this vehicle is on
func (v *Vehicle) MatchTripWithPV(pv *Vehicle, st time.Time, be []*brigadeEntry) error {
	// first - get the index in be of pv.Trip
	prevTripIdx := indexMatchingTrip(be, pv.Trip, pv.ServiceDate)

	// EDGE CASE: previous trip not found in brigadeEntries
	// calculate as if previous trip was not known
	if prevTripIdx < 0 {
		return v.MatchTripNoPV(st, be)
	}

	// extract info about previous vehicle trip
	pvTripEntry := be[prevTripIdx]
	secondsToEnd := pvTripEntry.LastStopTime.Sub(st).Seconds()

	// EDGE CASE: previous vehicle was doing the last trip of its duty -
	// either the last trip of the brigade, or the last one on its service date
	if prevTripIdx == len(be)-1 || be[prevTripIdx+1].ServiceDate != pvTripEntry.ServiceDate {
		// 15 minutes past last trip end time - assume the vehicle is no longer
		// active on the network
		if secondsToEnd < -900 {
			v.Trip = ""
			return nil
		}

		// Otherwise assume the vehicle is still doing the last trip
		v.setTrip(pvTripEntry)
		return nil
	}

	// The vehicle is "nearTerminus" if it's 50 meters to tripLastStop
	nearTerminus := haversine(v.Lat, v.Lon, pvTripEntry.LastStopPos[0], pvTripEntry.LastStopPos[1]) <= 0.05

	// The vehicle is "nearEndTime" if it's 4 minutes to or past the tripLastTime
	nearEndTime := secondsToEnd < 240

	// This is a fail-safe assumption that no trip is delayed more then 30 minutes
	isVeryLate := secondsToEnd < -1800

	// Move to the next trip if it's close to the trip end (in time and space) or
	// if the fail-safe is active
	if (nearTerminus && nearEndTime) || isVeryLate {
		v.setTrip(be[prevTripIdx+1])
	} else {
		v.setTrip(pvTripEntry)
	}
	return nil
}

// MatchTrip tries to guess which trip this vehicle is on
func (v *Vehicle) MatchTrip(pv *Vehicle, st time.Time, be []*brigadeEntry) error {
	// Handle some edge cases
	if len(be) == 0 {
		// No corresponding brigade entries - an 'inactive vehicle' -
		// don't set the v.Trip attribute (to mark this vehicle as ignorable)
		// and don't return any error states.
		return nil
	} else if pv == nil {
		// The previous entry for this vehicle id is unknown
		return v.MatchTripNoPV(st, be)
	}

	return v.MatchTripWithPV(pv, st, be)
}

// setTrip assigns a particular run of a trip to this vehicle
func (v *Vehicle) setTrip(b *brigadeEntry) {
	v.Trip = b.TripID
	v.ServiceDate = b.ServiceDate
}

// VehicleContainer is a container for multiple Vehicle objects
type VehicleContainer struct {
	SyncTime time.Time
	Vehicles map[string]*Vehicle
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
			Time:      vc.SyncTime.Format(time.RFC3339),
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

	err = os.WriteFile(target, buff, 0o666)
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
	// Marshall to GTFS-RT
	var b []byte
	if humanReadable {
		b, err = prototext.Marshal(vc.AsProto())
	} else {
		b, err = proto.Marshal(vc.AsProto())
	}

	// Check for marshall errors
	if err != nil {
		return
	}

	// Write to target file
	err = os.WriteFile(target, b, 0o666)
	return
}

// Prepare initializes the vehiclecontainer.Vehicles map with
// vehicle objects created from a sequence of apiVehicleEntry
func (vc *VehicleContainer) Prepare(apiEntries []*APIVehicleEntry) error {
	timezone, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		return err
	}

	vc.Vehicles = make(map[string]*Vehicle, len(apiEntries))

	for _, ae := range apiEntries {
		v, err := NewVehicle(ae, timezone)
		if err != nil {
			return err
		}
		vc.Vehicles[v.ID] = v
	}

	return nil
}

// MatchAll tries to calculate the bearing and to match a vehicle to a particular trip
// for all its Vehicles. If a Vehicle still has an empty Trip field after calling its Match
// function such vehicle is removed for the container.
func (vc *VehicleContainer) MatchAll(brigadeMap map[string][]*brigadeEntry, prevVehicles map[string]*Vehicle) error {
	for vID, v := range vc.Vehicles {
		// Don't match if the vehicle position is not realtime -
		// older then 3 minutes
		if vc.SyncTime.Sub(v.TimeObj).Minutes() > 3 {
			delete(vc.Vehicles, vID)
			continue
		}

		// Try to find matching brigade fields
		be := brigadeMap[vID]

		// Try to find previous Vehicle for this ID
		pv := prevVehicles[vID]

		// Match this vehicle to a particular trip
		err := v.MatchTrip(pv, vc.SyncTime, be)
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
