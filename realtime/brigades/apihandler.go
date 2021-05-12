package brigades

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// routeStopPait is a struct, for a (route_id, stop_id) pair
type routeStopPair struct{ Route, Stop string }

// mapTimeBrigade is an alias for a map from a timestamp to a brigade_id
type mapTimeBrigade = map[string]string

// invalidTTableAPIResp represents an invalid response from api.um.warszawa.pl
type invalidTTableAPIResp struct{ text string }

func (e invalidTTableAPIResp) Error() string {
	return e.text
}

// ttableAPI is an object for communcating with the timetable api at api.um.warszawa.pl
type ttableAPI struct {
	Key           string
	Client        *http.Client
	Respones      map[routeStopPair]mapTimeBrigade // routeStop → time → brigade
	ForwardErrors bool
}

// BuildURL returns the URL to retrieve timetables of a specific route-stop pair
func (api *ttableAPI) BuildURL(rs routeStopPair) string {
	queryParams := url.Values{}
	queryParams.Set("id", "e923fa0e-d96c-43f9-ae6e-60518c9f3238")
	queryParams.Set("apikey", api.Key)
	queryParams.Set("busstopId", rs.Stop[:4])
	queryParams.Set("busstopNr", rs.Stop[4:6])
	queryParams.Set("line", rs.Route)
	requestURL := url.URL{
		Scheme:   "https",
		Host:     "api.um.warszawa.pl",
		Path:     "/api/action/dbtimetable_get/",
		RawQuery: queryParams.Encode(),
	}
	return requestURL.String()
}

// Get returns the time→brigade map for a particular route-stop pair
func (api *ttableAPI) Get(rs routeStopPair) (mapTimeBrigade, bool, error) {
	// Check if this pair was defined earlier
	ttb, hasCached := api.Respones[rs]
	if hasCached {
		return ttb, true, nil
	}

	// Prepare request
	logPrintf("Making call for R %s | S %s", true, rs.Route, rs.Stop)
	requestURL := api.BuildURL(rs)
	resp, err := api.Client.Get(requestURL)
	if err != nil {
		return nil, false, err
	}

	defer resp.Body.Close()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		logPrintf("Timetable API for %+v responded with status code: %d", false, rs, resp.StatusCode)
		if api.ForwardErrors {
			err = util.RequestError{
				URL:        strings.ReplaceAll(requestURL, api.Key, "xxxxxx"),
				Status:     resp.Status,
				StatusCode: resp.StatusCode,
			}
		}
		return nil, false, err
	}

	// Read the response
	rawData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}

	// Parse the response
	ttb, err = parseBrigadesResponse(rawData, rs, api.ForwardErrors)
	if err != nil {
		api.Respones[rs] = make(mapTimeBrigade)
		return nil, false, err
	}

	api.Respones[rs] = ttb
	return ttb, false, nil
}

// parseBrigadesResponse parses UM Warszawa API response when requesting a timetable for specific
// route_id, stop_id pair
func parseBrigadesResponse(rawData []byte, rs routeStopPair, forwardErrors bool) (mtb mapTimeBrigade, err error) {
	mtb = make(mapTimeBrigade)

	// Unmarshall JSON
	var decodedData struct {
		Result []struct {
			Values []struct {
				Key   string
				Value string
			}
		}
	}

	err = json.Unmarshal(rawData, &decodedData)
	if err != nil {
		return
	}

	// Check if there actually is a timetable
	if len(decodedData.Result) == 0 {
		errInfo := fmt.Sprintf("Timetable API for %+v returned an empty departures list (%q)", rs, string(rawData))
		logPrint(errInfo, false)
		if forwardErrors {
			err = invalidTTableAPIResp{errInfo}
			return
		}
	}

	// Extract time→brigade mapping
	for _, result := range decodedData.Result {
		var brigade string
		var time string

		// try to find matching fields
		for _, value := range result.Values {
			if value.Key == "brygada" {
				brigade = value.Value
			} else if value.Key == "czas" {
				time = value.Value
			}
		}

		// check if "brygada" and "czas" fields exist
		if (brigade == "" || time == "") && forwardErrors {
			errInfo := fmt.Sprintf(
				"Timetable API for %+v returned a timetable with missing times or brigades (%q)",
				rs, string(rawData))
			logPrint(errInfo, false)
			if forwardErrors {
				err = invalidTTableAPIResp{errInfo}
				return
			}
		}

		mtb[time] = brigade
	}

	return
}
