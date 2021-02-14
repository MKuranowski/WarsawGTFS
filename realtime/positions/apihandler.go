package positions

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
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
	return fmt.Sprintf("api.um.warszawa.pl responded with an error: %q", e.apiErrMsg)
}

// VehicleAPI is an object for communicating with the vehicle position api at api.um.warszawa.pl
type VehicleAPI struct {
	Key    string
	Client *http.Client
}

// buildURL returs the url of API endpoint with vehicle data for given vehicle type
func (api *VehicleAPI) buildURL(apiVehType string) string {
	return fmt.Sprintf(
		"https://api.um.warszawa.pl/api/action/busestrams_get/"+
			"?resource_id=f2e5503e927d-4ad3-9500-4ab9e55deb59&apikey=%s&type=%s",
		url.QueryEscape(api.Key),
		url.QueryEscape(apiVehType),
	)
}

// Get tries to get vehicle positions from the API.
// apiVehType can be "1" to get bus positions or "2" to get tram positions.
func (api *VehicleAPI) Get(apiVehType string) ([]*APIVehicleEntry, error) {
	// Prepare request
	reqURL := api.buildURL(apiVehType)
	resp, err := api.Client.Get(reqURL)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		err = util.RequestError{
			URL:        strings.ReplaceAll(reqURL, api.Key, "xxxxxx"),
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
		}
		return nil, err
	}

	// Read the response
	respRaw, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Marshall the response into JSON
	var respJSON struct {
		Error  string
		Result []*APIVehicleEntry
	}

	err = json.Unmarshal(respRaw, &respJSON)
	if err != nil {
		log.Printf("Invalid API UM Respose for %s:\n%s\n", apiVehType, respRaw)
		return nil, err
	} else if respJSON.Error != "" {
		return nil, apiError{respJSON.Error}
	}

	return respJSON.Result, nil
}

// GetAll will automatically call the api to retrieve a list of all tram and bus positions
func (api *VehicleAPI) GetAll() (s []*APIVehicleEntry, err error) {
	// create used objects
	var tempBuff []*APIVehicleEntry

	// load tram positions
	tempBuff, err = api.Get("2")
	if err != nil {
		return
	}
	s = append(s, tempBuff...)

	// load bus positions
	tempBuff, err = api.Get("1")
	if err != nil {
		return
	}
	s = append(s, tempBuff...)
	return
}
