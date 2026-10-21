package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

var WarsawTimezone *time.Location

func init() {
	var err error
	WarsawTimezone, err = time.LoadLocation("Europe/Warsaw")
	if err != nil {
		panic(fmt.Errorf("failed to load Europe/Warsaw timezone: %w", err))
	}
}

// MakeTranslatedString takes a string and warps it into a gtfs-realtime TranslatedString object
func MakeTranslatedString(s string) *gtfsrt.TranslatedString {
	return &gtfsrt.TranslatedString{
		Translation: []*gtfsrt.TranslatedString_Translation{
			{Text: &s},
		},
	}
}

// MakeFeedMessage preapres a GTFS-RT FeedMessage object and adds a valid FeedHeader to it
func MakeFeedMessage(t time.Time) *gtfsrt.FeedMessage {
	ver := "2.0"
	incr := gtfsrt.FeedHeader_FULL_DATASET
	tstamp := uint64(t.UTC().Unix())
	return &gtfsrt.FeedMessage{
		Header: &gtfsrt.FeedHeader{
			GtfsRealtimeVersion: &ver,
			Incrementality:      &incr,
			Timestamp:           &tstamp,
		},
	}
}

// StringSliceHas checks if element is inside a StringSlice
func StringSliceHas(s sort.StringSlice, x string) bool {
	maxIdx := s.Len()
	searchIdx := s.Search(x)
	if searchIdx >= maxIdx || s[searchIdx] != x {
		return false
	}
	return true
}

// StringSliceInsert adds x into sort.StringSlice
func StringSliceInsert(s sort.StringSlice, x string) sort.StringSlice {
	maxIdx := s.Len()
	searchIdx := s.Search(x)
	if searchIdx >= maxIdx {
		s = append(s, x)
	} else if s[searchIdx] != x {
		s = append(s, "")
		copy(s[searchIdx+1:], s[searchIdx:])
		s[searchIdx] = x
	}
	return s
}

// ZipStrings maps elements from a to elements from b, such that
// a[i] in the returned map to b[i]
func ZipStrings(a []string, b []string) (zipped map[string]string) {
	zipped = make(map[string]string, len(a))
	for idx, elem := range a {
		zipped[elem] = b[idx]
	}
	return
}

// ParseTimeToSeconds parses a HH:MM[:SS] string into a single number representing a
// total number of seconds.
func ParseTimeToSeconds(x string) (uint32, error) {
	parts := strings.Split(x, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, InvalidTimeString{x}
	}

	h, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil {
		return 0, InvalidTimeString{x}
	}

	m, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return 0, InvalidTimeString{x}
	}

	s := uint64(0)
	if len(parts) == 3 {
		s, err = strconv.ParseUint(parts[2], 10, 8)
		if err != nil {
			return 0, InvalidTimeString{x}
		}
	}

	return uint32(h*3600 + m*60 + s), nil
}

func SecondsToString(totalSeconds uint32) string {
	totalMinutes, s := totalSeconds/60, totalSeconds%60
	h, m := totalMinutes/60, totalMinutes%60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// ServiceDates returns the service dates (as YYYYMMDD strings) whose trips might be running
// at t: the day before (for trips past midnight, with times like 27:15:00) and the day of t.
func ServiceDates(t time.Time) []string {
	t = t.In(WarsawTimezone)
	return []string{t.AddDate(0, 0, -1).Format("20060102"), t.Format("20060102")}
}

// GtfsTime converts a GTFS time (HH:MM:SS, possibly over 24:00:00) on a given
// service date (YYYYMMDD) into an absolute time.Time. GTFS times are measured from
// "noon minus 12h", which keeps them correct on DST change days.
func GtfsTime(serviceDate string, hms string) (time.Time, error) {
	d, err := time.ParseInLocation("20060102", serviceDate, WarsawTimezone)
	if err != nil {
		return time.Time{}, err
	}

	seconds, err := ParseTimeToSeconds(hms)
	if err != nil {
		return time.Time{}, err
	}

	noon := time.Date(d.Year(), d.Month(), d.Day(), 12, 0, 0, 0, WarsawTimezone)
	return noon.Add(-12*time.Hour + time.Duration(seconds)*time.Second), nil
}
