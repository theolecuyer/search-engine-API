package lib

import (
	"log"
	"net/url"
	"strings"
)

func Clean(host string, hrefs []string) []*url.URL {
	var cleaned []*url.URL
	u, err := url.Parse(host)
	if err != nil {
		log.Fatalf("url.Parse 1 returned %v\n", err)
	}
	for _, href := range hrefs {
		if u.Host != "localhost:8080" {
			href = strings.ReplaceAll(href, " ", "")
			href = strings.ReplaceAll(href, "\n", "")
		}
		href = strings.TrimLeft(href, " ")
		href = strings.ReplaceAll(href, " ", "%20")
		href = strings.ReplaceAll(href, "%&", "%25&")
		hr, err := url.Parse(href)
		if err != nil {
			log.Fatalf("url.Parse returned %v\n", err)
		}
		//If the hrefs is absolute, or its own valid URL, resolve reference will handle the case
		//Used net/url documentation to find the resolve reference method
		newUrl := u.ResolveReference(hr)
		cleaned = append(cleaned, newUrl)
	}
	return cleaned
}
