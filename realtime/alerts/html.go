package alerts

import (
	"net/url"
	"strings"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/MKuranowski/WarsawGTFS/realtime/util"
	"github.com/PuerkitoBio/goquery"
	"github.com/microcosm-cc/bluemonday"
)

// getSanitizePolicy creates a default policy for html sanitization
func getSanitizePolicy() (p *bluemonday.Policy) {
	p = bluemonday.NewPolicy()
	p.AllowElements(
		"p",
		"span",
		"blockquote",
		"h1",
		"h2",
		"h3",
		"h4",
		"h5",
		"br",
		"hr",
	)

	// Allow lists + 'start' attributes on ordered lists
	p.AllowLists()
	p.AllowAttrs("start").Matching(bluemonday.Integer).OnElements("ol")

	// Allow text decorations
	p.AllowElements(
		"strong", // strong (bold)
		"em",     // emphasis (cursive)
		"s",      // strikethrough
	)

	// Allow very specific styling:
	// color on spans and some text-decorations
	p.AllowStyles("color").Matching(regexpColor).OnElements("span")
	p.AllowStyles("text-decoration").MatchingEnum("underline").OnElements("span")

	return
}

// getMarkdownConverter creates a default markdown converter from **sanitized** html to
// a kinda-plaintext string.
func getMarkdownConverter() (conv *md.Converter) {
	conv = md.NewConverter("", true, &md.Options{HeadingStyle: "setext"})

	// Striketrough text should be deleted
	conv.Remove("s")

	conv.AddRules(
		// Text decorations should be ignored
		md.Rule{
			Filter: []string{"strong", "em", "span"},
			Replacement: func(content string, selec *goquery.Selection, options *md.Options) *string {
				return &content
			},
		},

		// Blockqoutes should be indented
		md.Rule{
			Filter: []string{"blockquote"},
			Replacement: func(content string, selec *goquery.Selection, options *md.Options) *string {
				var newContent string
				for _, line := range strings.Split(content, "\n") {
					newContent += "  " + line
				}
				return &newContent
			},
		},
	)
	return
}

// getWebsite downloads a website with an alert description,
// and passes the website to goquery.
func getWebsite(client exclusiveHTTPClient, rawurl string, alertID string) (doc *goquery.Document, err error) {
	// Parse the URL
	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		return
	}

	// Check if it points to wtp.waw.pl
	if parsedURL.Host != "www.wtp.waw.pl" && parsedURL.Host != "wtp.waw.pl" {
		err = util.UnexpectedHost{Where: alertID, Host: parsedURL.Host, Expected: "wtp.waw.pl"}
		return
	}

	// Request the url
	resp, err := client.Get(rawurl)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		err = util.RequestError{URL: rawurl, Status: resp.Status, StatusCode: resp.StatusCode}
		return
	}

	// Parse HTML
	doc, err = goquery.NewDocumentFromReader(resp.Body)
	return
}

// getAlertFlags determines which flags are set for this alert.
// Flags usually represent which routes are affected by an alert.
func getAlertFlags(doc *goquery.Document, alertType string) (flags []string) {
	// CSS class for flag icons is different in impediments and changes.
	var selector string
	if alertType == "REDUCED_SERVICE" {
		selector = ".impediment-category-icon"
	} else {
		selector = ".format-icon"
	}

	doc.Find(selector).Each(func(i int, s *goquery.Selection) {
		alt, ok := s.Attr("alt")
		if ok {
			flags = append(flags, alt)
		}
	})
	return
}

// getAlertDesc tries to make out the actual content of an alert.
// Returns sanitized HTML (via the provided policy).
func getAlertDesc(doc *goquery.Document, alertType string) (htmlBody string, err error) {
	var main *goquery.Selection
	if alertType == "REDUCED_SERVICE" {
		main = doc.Find("div.impediment-content")
	} else {
		main = doc.Find("main.page-main")
		main.Find("div.format-sidebar-pinned").PrevAll().AddBack().Remove()
		main.Find("div.is-style-small").Remove()
	}

	// Remove everything after <hr>
	main.Find("hr").NextAll().AddBack().Remove()

	// Assume there's only one main element in `main`
	if main.Length() != 1 {
		return
	}

	// Dump found alert content
	htmlBody, err = main.Html()
	if err != nil {
		return
	}

	// Sanitize HTML
	htmlBody = htmlSanitizer.Sanitize(htmlBody)
	htmlBody = strings.Trim(htmlBody, "\n\t ")
	return
}

// getAlertPlaintext returns markdown version of an alert.
func getAlertPlaintext(htmlBody string) (string, error) {
	return markdownConverter.ConvertString(htmlBody)
}
