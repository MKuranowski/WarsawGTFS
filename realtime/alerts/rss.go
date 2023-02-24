package alerts

import (
	"encoding/xml"
	"io"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// rssItem represents a subset of RSS <item> element
type rssItem struct {
	XMLName     xml.Name `xml:"item"`
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	PubDate     string   `xml:"pubDate"`
	GUID        string   `xml:"guid"`
	Description string   `xml:"description"`
	Type        string   `xml:"-"`
}

// rssRoot represents the root of an RSS feed
type rssRoot struct {
	XMLName xml.Name `xml:"rss"`
	Channel struct {
		XMLName xml.Name   `xml:"channel"`
		Items   []*rssItem `xml:"item"`
	} `xml:"channel"`
}

// unmarshalRss parses a binary buffer with rss content in it and returns an rssRoot object.
// If 'assignItemType' is set, every rssItem in channel.Items
// will have its 'Type' field set to the provided value
func unmarshalRss(data []byte, assignItemType string) (*rssRoot, error) {
	var rootRss rssRoot
	err := xml.Unmarshal(data, &rootRss)
	if err != nil {
		return nil, err
	}

	// Assign assignItemType to all RssItems
	if assignItemType != "" {
		for idx := range rootRss.Channel.Items {
			rootRss.Channel.Items[idx].Type = assignItemType
		}
	}

	return &rootRss, nil
}

// getRss requests a given url using the provided http.Client and parses the response
// expecting a valid RSS in return.
// If 'assignItemType' is set, every rssItem in channel.Items
// will have its 'Type' field set to the provided value
func getRss(client exclusiveHTTPClient, url string, assignItemType string) (*rssRoot, error) {
	// Request the url
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		return nil, util.RequestError{URL: url, Status: resp.Status, StatusCode: resp.StatusCode}
	}

	// Read the response
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Unmarshall RSS
	channel, err := unmarshalRss(content, assignItemType)
	if err != nil {
		return nil, err
	}

	return channel, nil
}
