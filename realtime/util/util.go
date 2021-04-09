package util

import (
	"sort"
	"time"

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

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
