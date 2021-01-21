package positions

import (
	"fmt"
	"time"
)

type invalidCompareTimeComparison struct {
	t compareTime
}

func (e invalidCompareTimeComparison) Error() string {
	return "Uncertain compareTime can't be the root of comparison"
}

// compareTime is an object representing a certain time of day.
// compareTime objects are designed to deal with uncertainty with day offsets
type compareTime struct {
	h, m, s      int
	uncertainDay bool
}

// newcompareTimeFromGtfs creates a certain compareTime object from a "HH:MM:SS" GTFS timepoint
func newCompareTimeFromGtfs(hms string) (t compareTime, e error) {
	t.uncertainDay = false
	_, e = fmt.Sscanf(hms, "%d:%d:%d", &t.h, &t.m, &t.s)
	return
}

// newcompareTimeFromTime creates an uncertain compareTime object from a time.Time object
func newCompareTimeFromTime(tObj time.Time) (t compareTime) {
	t.uncertainDay = true
	t.h, t.m, t.s = tObj.Clock()
	return
}

// Seconds returns the seconds-since-midnight count of a compareTime object
func (t compareTime) Seconds() int {
	return t.s + t.m*60 + t.h*3600
}

// After checks if compareTime happened after other
func (t compareTime) After(other compareTime) (b bool, err error) {
	// t has to be a certain compareTime object
	if t.uncertainDay {
		err = invalidCompareTimeComparison{t}
		return
	}

	tSec := t.Seconds()
	oSec := other.Seconds()

	// fix midnight rollover
	if other.uncertainDay && t.h >= 24 && other.h <= 3 {
		oSec += 86400
	}

	b = tSec > oSec
	return
}

// Until returns how many seconds passed to t since other, taking midnight rollover into account
func (t compareTime) Since(other compareTime) (s int, err error) {
	// t has to be a certain compareTime object
	if t.uncertainDay {
		err = invalidCompareTimeComparison{t}
		return
	}

	tSec := t.Seconds()
	oSec := other.Seconds()

	// fix midnight rollover
	if other.uncertainDay && t.h >= 24 && other.h <= 3 {
		oSec += 86400
	}

	s = tSec - oSec
	return
}
