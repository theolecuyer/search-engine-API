package lib

import (
	"log"
	"strings"
	"unicode"

	"github.com/Diggernaut/stopwords"
	"golang.org/x/net/html"
)

func Extract(body string) ([]string, []string) {
	//Make the parser object
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		log.Fatalf("html.Parse returned %v\n", err)
	}
	var words []string
	var hrefs []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.TextNode && n.Parent.Data != "style" {
			// Remove non-letter and non-number characters
			stringSlice := strings.FieldsFunc(n.Data, func(r rune) bool {
				return (!unicode.IsLetter(r) && !unicode.IsNumber(r))
			})

			// Clean each word to filter out stopwords
			for _, word := range stringSlice {
				cleanedWord := stopwords.CleanString(word, "en", true)
				if string(strings.TrimSpace(cleanedWord)) != "" {
					words = append(words, string(cleanedWord))
				}
			}
		}

		//Get hrefs from the parser
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					//Get rid of fragment identifiers
					if strings.HasPrefix(a.Val, "#") {
						break
					}
					hrefs = append(hrefs, a.Val)
					break
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return words, hrefs
}
