package alerts

import (
	"regexp"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/microcosm-cc/bluemonday"
)

var htmlCleaner *bluemonday.Policy = bluemonday.StrictPolicy()
var htmlSanitizer *bluemonday.Policy = getSanitizePolicy()

var markdownConverter *md.Converter = getMarkdownConverter()

var regexpColor *regexp.Regexp = regexp.MustCompile("(?i)^#([0-9a-f]{3,4}|[0-9a-f]{6}|[0-9a-f]{8})$")
var regexID *regexp.Regexp = regexp.MustCompile(`&p=(\d+)`)
var regexRoute *regexp.Regexp = regexp.MustCompile(`[0-9A-Za-z-]{1,3}`)

const urlChanges string = "https://www.wtp.waw.pl/feed/?post_type=change"
const urlImpediments string = "https://www.wtp.waw.pl/feed/?post_type=impediment"
