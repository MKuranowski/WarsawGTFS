package positions

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

const (
	apiBus  = 1
	apiTram = 2
	apiSKM  = -1
)

// APIVehicleEntry represents a single object from the API
type APIVehicleEntry struct {
	Lat, Lon                            float64
	Time, Lines, Brigade, VehicleNumber string
}

// apiError is an error object used when the API returns an error
type apiError struct {
	apiErrMsg string
}

func (e apiError) Error() string {
	return fmt.Sprintf("dane.um.warszawa.pl responded with an error: %q", e.apiErrMsg)
}

// VehicleAPI is an object for communicating with the vehicle position api at dane.um.warszawa.pl
type VehicleAPI struct {
	Key    string
	Client *http.Client
}

// buildWarsawRequest prepares a http.Request for fetching vehicles from dane.um.warszawa.pl
// of a particular kind.
func (api *VehicleAPI) buildWarsawRequest(apiVehType int) *http.Request {
	body := strings.NewReader(fmt.Sprintf(`{"type":%d}`, apiVehType))
	req, err := http.NewRequest("POST", "https://dane.um.warszawa.pl/api/action/get_ztm_lokalizacja_pojazdow", body)
	if err != nil {
		panic(fmt.Errorf("http.NewRequest: dane.um.warszawa.pl: %w", err))
	}
	req.Header.Add("Authorization", api.Key)
	req.Header.Add("Content-Type", "application/json")
	return req
}

// buildZbiorkomRequest prepares a http.Request for fetching SKM vehicles from zbiorkom.live
func (api *VehicleAPI) buildZbiorkomRequest() *http.Request {
	req, err := http.NewRequest("GET", "https://api.zbiorkom.live/4.9/external/skm", nil)
	if err != nil {
		panic(fmt.Errorf("http.NewRequest: api.zbiorkom.live: %w", err))
	}
	return req
}

func (api *VehicleAPI) buildRequest(apiVehType int) *http.Request {
	if apiVehType < 0 {
		return api.buildZbiorkomRequest()
	}
	return api.buildWarsawRequest(apiVehType)
}

// Get tries to get vehicle positions from the API.
// apiVehType can be "1" to get bus positions or "2" to get tram positions.
func (api *VehicleAPI) Get(apiVehType int) ([]*APIVehicleEntry, error) {
	// Prepare request
	req := api.buildRequest(apiVehType)
	resp, err := api.Client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		err = util.RequestError{
			URL:        req.URL.String(),
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
		}
		return nil, err
	}

	// Read the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data, err := tryParseResponse(body)
	if err != nil {
		log.Printf("Invalid %s response for %d:\n%s\n", req.URL.Host, apiVehType, body)
		return nil, err
	}
	return data, nil
}

// GetAll will automatically call the api to retrieve a list of all tram, bus and SKM positions
func (api *VehicleAPI) GetAll() (s []*APIVehicleEntry, err error) {
	var tmp []*APIVehicleEntry

	for _, apiType := range []int{apiTram, apiBus, apiSKM} {
		tmp, err = api.Get(apiType)
		if err != nil {
			return
		}
		s = append(s, tmp...)
	}

	return
}

func tryParseResponse(data []byte) ([]*APIVehicleEntry, error) {
	// Generally, we expect a {"error": "error description"} or a {"result": [...]} object,
	// but the API sometimes simply returns `[...]` or `"error description"`.
	var resp struct {
		Error  string
		Result []*APIVehicleEntry
	}

	err := json.Unmarshal(data, &resp)
	if e, ok := err.(*json.UnmarshalTypeError); ok && e.Field == "" {
		// If unmarshal into an object fails at the top-level (e.Field == ""),
		// try to parse directly a slice or string
		altOk := false
		switch e.Value {
		case "array":
			altErr := json.Unmarshal(data, &resp.Result)
			altOk = altErr == nil

		case "string":
			altErr := json.Unmarshal(data, &resp.Error)
			altOk = altErr == nil

		default:
		}

		if altOk {
			err = nil
		}
	}

	// Check if an error occurred
	if err != nil {
		return nil, err
	} else if resp.Error != "" {
		return nil, apiError{resp.Error}
	}

	return resp.Result, nil
}
