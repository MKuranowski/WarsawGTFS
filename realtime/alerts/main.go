package alerts

import (
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Options represents available options for generating alerts
type Options struct {
	GtfsRtTarget    string
	JSONTarget      string
	HumanReadable   bool
	ThrowLinkErrors bool
}

// exclusiveHttpClient is a pair of *html.Client and *sync.Mutex
// to avoid spamming a single host with requests
type exclusiveHTTPClient struct {
	m *sync.Mutex
	c *http.Client
}

func (client exclusiveHTTPClient) Get(url string) (resp *http.Response, err error) {
	client.m.Lock()
	defer client.m.Unlock()
	return client.c.Get(url)
}

// allRssItems fetches urlImpediments and urlChanges and retrieves all
// RssItems that should be converted into Alerts
func allRssItems(client exclusiveHTTPClient) (items []*rssItem, err error) {
	// Load both RSS feeds
	impedimentsRss, err := getRss(client, urlImpediments, "REDUCED_SERVICE")
	if err != nil {
		return
	}

	changesRss, err := getRss(client, urlChanges, "OTHER_EFFECT")
	if err != nil {
		return
	}

	// Make a slice for all RssItems
	items = make([]*rssItem, 0, len(changesRss.Channel.Items)+len(impedimentsRss.Channel.Items))
	for _, impedimentItem := range impedimentsRss.Channel.Items {
		items = append(items, impedimentItem)
	}
	for _, changeItem := range changesRss.Channel.Items {
		items = append(items, changeItem)
	}

	return
}

// Make auto-magically creates GTFS-Realtime feeds with alert data
func Make(client *http.Client, routeMap map[string]sort.StringSlice, opts Options) (err error) {
	// Create a container for all Alerts
	var container AlertContainer
	container.Timestamp = time.Now()
	container.Time = container.Timestamp.Format("2006-01-02 15:04:05")

	// Wrap the http.Client exclusiveHTTPClient to avoid spamming wtp.waw.pl
	exclusiveClient := exclusiveHTTPClient{
		m: &sync.Mutex{},
		c: client,
	}

	// Load both RSS feeds
	log.Println("Fetching RSS feeds")
	items, err := allRssItems(exclusiveClient)
	if err != nil {
		return
	}

	// Convert RSS items to Alert objects
	log.Println("Casting RSS items to Alert objects")
	for _, item := range items {
		var a *Alert
		a, err = alertFromRssItem(item)
		if err != nil {
			return
		}

		container.Alerts = append(container.Alerts, a)
	}

	// Load data from alert links
	err = container.LoadExternal(exclusiveClient, routeMap, opts.ThrowLinkErrors)
	if err != nil {
		return
	}

	// Export to a JSON file
	if opts.JSONTarget != "" {
		log.Println("Exporting to JSON")
		container.SaveJSON(opts.JSONTarget)
	}

	// Export to a GTFS-Realtime file
	if opts.GtfsRtTarget != "" {
		log.Println("Exporting to GTFS-Realtime")
		container.SavePB(opts.GtfsRtTarget, opts.HumanReadable)
	}
	return
}
