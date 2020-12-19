package util

import "fmt"

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
