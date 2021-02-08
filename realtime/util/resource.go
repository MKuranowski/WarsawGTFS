package util

import (
	"io"
	"net/http"
	"os"
	"time"
)

// parseLastModified parses a Last-Modified header value to a time.Time object
func parseLastModified(lm string) (t time.Time, err error) {
	// Try to parse as RFC1123
	t, err = time.Parse(time.RFC1123, lm)
	if err != nil {
		// Try to parse as RFC1123Z
		t, err = time.Parse(time.RFC1123Z, lm)
	}
	// We compare times in UTC
	if err != nil {
		t = t.UTC()
	}
	return
}

// Resource interface represents an external, fetchable resource:
// Check() should check if the resource should be updated,
// Fetch() fetches the resource.
type Resource interface {
	Check() (bool, error)
	Fetch() (io.ReadCloser, error)
}

// ResourceLocal is a Resource located on the file system
type ResourceLocal struct {
	Path            string
	Checktime       time.Time
	Peroid          time.Duration
	FetchedModified time.Time
}

// Check checks if the file has changed, but only if
// more then r.Peroid has passed since r.Fetchtime
func (r *ResourceLocal) Check() (fetch bool, err error) {
	// If the nextCheckTime (r.Checktime + r.Peroid) is in the future,
	// abort any check and return false
	if r.Checktime.Add(r.Peroid).After(time.Now().UTC()) {
		return
	}

	// Stat the file
	info, err := os.Stat(r.Path)
	if err != nil {
		return
	}

	// Check if file was modified after current fetch time
	fetch = info.ModTime().UTC().After(r.FetchedModified)

	// Update last successful check time
	r.Checktime = time.Now().UTC()
	return
}

// Fetch returns the ReadCoser with access to the underlaying file
func (r *ResourceLocal) Fetch() (io.ReadCloser, error) {
	// Open the file
	f, err := os.Open(r.Path)
	if err != nil {
		return nil, err
	}

	// Stat it to update the FetchedModified attribute
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	r.FetchedModified = info.ModTime().UTC()
	return f, nil
}

// ResourceHTTP is a Resource located on the internet, fetchable via HTTP/HTTPS
type ResourceHTTP struct {
	Client          *http.Client
	URL             string
	Checktime       time.Time
	Peroid          time.Duration
	FetchedETag     string
	FetchedModified time.Time
}

// Check checks if the file on the internet has changed, but only if
// more then r.Peroid has passed since r.Fetchtime
func (r *ResourceHTTP) Check() (fetch bool, err error) {
	// If the nextCheckTime (r.Checktime + r.Peroid) is in the future,
	// abort any check and return false
	if r.Checktime.Add(r.Peroid).After(time.Now().UTC()) {
		return
	}

	// make a HEAD request to check if the file has changed
	resp, err := r.Client.Head(r.URL)
	if err != nil {
		return
	}
	resp.Body.Close()

	// Extract remote ETag and Last-Modified
	remoteETag := resp.Header.Get("ETag")
	remoteModified, err := parseLastModified(resp.Header.Get("Last-Modified"))
	if err != nil {
		return
	}

	if r.FetchedETag != "" && remoteETag != "" {
		// If using ETag, mark resource as changed if the ETag has changed
		fetch = r.FetchedETag != remoteETag
	} else {
		// If using Last-Modified, check if the resource has changed since
		// the last time a check was performed
		fetch = remoteModified.After(r.FetchedModified)
	}

	// Update last successful check time
	r.Checktime = time.Now().UTC()

	return
}

// Fetch returns the ReadCoser with access to the underlaying file
func (r *ResourceHTTP) Fetch() (io.ReadCloser, error) {
	// Make the get request
	resp, err := r.Client.Get(r.URL)
	if err != nil {
		return nil, err
	}

	// Try to get metadata
	remoteETag := resp.Header.Get("ETag")
	remoteModified, err := parseLastModified(resp.Header.Get("Last-Modified"))
	if err != nil {
		resp.Body.Close()
		return nil, err
	}

	// Save metadata
	r.FetchedETag = remoteETag
	r.FetchedModified = remoteModified

	// Return response content
	return resp.Body, nil
}
