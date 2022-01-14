package alerts

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	util "github.com/MKuranowski/WarsawGTFS/realtime/util"
	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/golang/protobuf/proto"
)

// Alert contains an internal representation of an alert, which is also marshallable to JSON
type Alert struct {
	ID       string   `json:"id"`
	Routes   []string `json:"routes"`
	Effect   string   `json:"effect"`
	Link     string   `json:"link"`
	Title    string   `json:"title"`
	Body     string   `json:"body"`
	HTMLBody string   `json:"htmlbody"`
}

// alertFromRssItem extracts basic data from an RssItem and puts them into an Alert
func alertFromRssItem(r *rssItem, routeMap map[string]sort.StringSlice) (a *Alert, err error) {
	a = &Alert{}

	// Extract the ID
	if idMatch := regexID.FindStringIndex(r.GUID); idMatch != nil {
		// Generate a prefix for the ID
		var idPrefix string
		if r.Type == "REDUCED_SERVICE" {
			idPrefix = "A/IMPEDIMENT/"
		} else {
			idPrefix = "A/CHANGE/"
		}

		a.ID = idPrefix + r.GUID[idMatch[0]+3:idMatch[1]]
	} else {
		err = fmt.Errorf("Unable to find alert ID in GUID %q", r.GUID)
		return
	}

	// Extract other data
	a.Effect = r.Type
	a.Link = htmlCleaner.Sanitize(r.Link)
	a.Title = htmlCleaner.Sanitize(r.Description)

	// Extract affected routes from the title
	if strings.Contains(r.Title, ":") {
		routesString := strings.SplitN(r.Title, ":", 2)[1]

		for _, route := range regexRoute.FindAllString(routesString, -1) {
			validRoute := false

			// Check if the route is mentioned in the GTFS
			for _, routeSubList := range routeMap {
				validRoute = validRoute || util.StringSliceHas(routeSubList, route)
			}

			if validRoute {
				a.Routes = append(a.Routes, route)
			}
		}
	}

	return
}

// makeEntitySelector creates a GTFS-RT []*EntitySelector that "select" applicable routes
func (a *Alert) makeEntitySelector() []*gtfsrt.EntitySelector {
	var entities []*gtfsrt.EntitySelector

	for i := range a.Routes {
		entities = append(entities, &gtfsrt.EntitySelector{RouteId: &a.Routes[i]})
	}

	return entities
}

// makeAlertEffect creates a GTFS-RT *Alert_Effect that selects the GTFS-RT Effect attribute
func (a *Alert) makeAlertEffect() *gtfsrt.Alert_Effect {
	var effect gtfsrt.Alert_Effect
	if a.Effect == "REDUCED_SERVICE" {
		effect = gtfsrt.Alert_REDUCED_SERVICE
	} else {
		effect = gtfsrt.Alert_OTHER_EFFECT
	}
	return &effect
}

// AsProto returns the Alert marshalled to a GTFS-RT FeedEntity
func (a *Alert) AsProto() *gtfsrt.FeedEntity {
	return &gtfsrt.FeedEntity{
		Id: &a.ID,
		Alert: &gtfsrt.Alert{
			InformedEntity:  a.makeEntitySelector(),
			Effect:          a.makeAlertEffect(),
			Url:             util.MakeTranslatedString(a.Link),
			HeaderText:      util.MakeTranslatedString(a.Title),
			DescriptionText: util.MakeTranslatedString(a.Body),
		},
	}
}

// LoadExternal processes data located on the website saved in a.Link
func (a *Alert) LoadExternal(client exclusiveHTTPClient, routeMap map[string]sort.StringSlice) (err error) {
	doc, err := getWebsite(client, a.Link, a.ID)
	if err != nil {
		return
	}

	// Process flags
	if len(a.Routes) <= 0 {
		flags := getAlertFlags(doc, a.Effect)
		for _, flag := range flags {
			switch flag {
			case "metro":
				a.Routes = append(a.Routes, routeMap["1"]...)
			case "tramwaje":
				a.Routes = append(a.Routes, routeMap["0"]...)
			case "skm", "kolej":
				a.Routes = append(a.Routes, routeMap["2"]...)
			case "autobusy":
				a.Routes = append(a.Routes, routeMap["3"]...)
			}
		}
	}

	// Sanitize the document to make a usable htmlBody
	a.HTMLBody, err = getAlertDesc(doc, a.Effect)
	if err != nil {
		return
	}

	// Strip the cleaned HTMLBody to make a plaintext description
	a.Body, err = getAlertPlaintext(a.HTMLBody)
	return
}

// AlertContainer is a container for multiple alerts, marshallable to JSON
type AlertContainer struct {
	Timestamp time.Time `json:"-"`
	Time      string    `json:"time"`
	Alerts    []*Alert  `json:"alerts"`
}

// AsProto returns this list of alerts marshalled into a GTFS-RT FeedMessage
func (ac *AlertContainer) AsProto() *gtfsrt.FeedMessage {
	msg := util.MakeFeedMessage(ac.Timestamp)
	for _, alert := range ac.Alerts {
		msg.Entity = append(msg.Entity, alert.AsProto())
	}
	return msg
}

// LoadExternal asynchronously calls LoadExternal on all its alerts
func (ac *AlertContainer) LoadExternal(client exclusiveHTTPClient, routeMap map[string]sort.StringSlice, throwErrors bool) error {
	// Make synchronization primitives
	wg := &sync.WaitGroup{}
	errCh := make(chan error, len(ac.Alerts)+1)

	// Make a goroutine for all LoadExternal
	for _, a := range ac.Alerts {
		// Log & update the waitgroup
		log.Printf("Fetching alert desc from %v\n", a.Link)
		wg.Add(1)

		// Call a goroutine to load external data
		go func(a *Alert) {
			defer wg.Done()
			err := a.LoadExternal(client, routeMap)

			// errors are only passed through if requested and if not nil
			if throwErrors && err != nil {
				errCh <- err
			}
		}(a)
	}

	// Wait until all goroutines finished
	wg.Wait()

	// Mark the end of possible errors
	close(errCh)

	// Check if any error was asked to be re-thrown
	for err := range errCh {
		return err
	}
	return nil
}

// Filter removes all alerts without associated routes
func (ac *AlertContainer) Filter() {
	filtered := ac.Alerts[:0]
	// Filter alerts
	for _, a := range ac.Alerts {
		if len(a.Routes) > 0 {
			filtered = append(filtered, a)
		}
	}
	// Garbage collect deleted alerts
	for i := len(filtered); i < len(ac.Alerts); i++ {
		ac.Alerts[i] = nil
	}
	// Set the filtered slice
	ac.Alerts = filtered
}

// SaveJSON marshalls the container into a json file at the given location
func (ac *AlertContainer) SaveJSON(target string) (err error) {
	// Open target file
	f, err := os.Create(target)
	if err != nil {
		return
	}
	defer f.Close()

	// Marshall JSON
	b, err := json.MarshalIndent(ac, "", "  ")
	if err != nil {
		return
	}
	f.Write(b)

	return
}

// SavePB marshalls the container into a GTFS-Realtime protocol buffer file
func (ac *AlertContainer) SavePB(target string, humanReadable bool) (err error) {
	// Open target file
	f, err := os.Create(target)
	if err != nil {
		return
	}
	defer f.Close()

	// Marshall to GTFS-RT
	if humanReadable {
		// Human-readable format
		err = proto.MarshalText(f, ac.AsProto())
		if err != nil {
			return
		}
	} else {
		// Binary format
		var b []byte
		b, err = proto.Marshal(ac.AsProto())
		if err != nil {
			return
		}
		f.Write(b)
	}

	return
}
