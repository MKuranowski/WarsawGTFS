package util

import (
	"fmt"
	"strings"
)

// RequestError is an error object used when a server responds with an unexpected return code
type RequestError struct {
	URL        string
	Status     string
	StatusCode int
}

func (e RequestError) Error() string {
	return fmt.Sprintf("%q responded with status %q", e.URL, e.Status)
}

// UnexpectedHost is an error object used when a link points to an invalid host
type UnexpectedHost struct {
	Where    string
	Host     string
	Expected string
}

func (e UnexpectedHost) Error() string {
	return fmt.Sprintf(
		"%q has a link to page on %q, but host %q was expected",
		e.Where, e.Host, e.Expected)
}

// InvalidGtfsReference is an error object used when one GTFS file references an non-existant field
type InvalidGtfsReference struct {
	ReferingFile       string
	Column             string
	Value              string
	ExpectedDefinition string
}

func (e InvalidGtfsReference) Error() string {
	return fmt.Sprintf(
		"file %s references %s=%q, which wasn't found in %s",
		e.ReferingFile,
		e.Column,
		e.Value,
		e.ExpectedDefinition,
	)
}

// MissingColumn is an error object used when a CSV file is missing a required column
type MissingColumn struct {
	File     string
	Required []string
	Missing  []string
}

func (e MissingColumn) Error() string {
	return fmt.Sprintf(
		"%s is missing several columns: %s",
		e.File,
		strings.Join(e.Missing, ", "),
	)
}

// MissingColumnCheck returns nil if all required columns are set in the given row,
// returns an MissingColumn error otherwise
func MissingColumnCheck(file string, required []string, row map[string]string) error {
	mce := MissingColumn{}

	// Check if any columns are missing
	for _, requiredCol := range required {
		_, has := row[requiredCol]
		if !has {
			mce.Missing = append(mce.Missing, requiredCol)
		}
	}

	// If any columns were missing, return a mce-error
	if len(mce.Missing) > 0 {
		mce.File = file
		mce.Required = required
		return mce
	}

	return nil
}

// InvalidTimeString is an error returned by [util.ParseTime] on invalid input strings
type InvalidTimeString struct {
	Input string
}

func (e InvalidTimeString) Error() string {
	return fmt.Sprintf("invalid time string: %q", e.Input)
}
