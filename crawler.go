package main

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var wikiLinkRegex = regexp.MustCompile(`href="/wiki/([^:#\"'<>]+)"`)

// CrawlPage fetches a Wikipedia page and extracts wiki links.
func CrawlPage(url string) ([]string, int, error) {
	client := &http.Client{Timeout: 4 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "GopherConUIBot/1.0 (contact@example.com) Go-LLM-Project")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	matches := wikiLinkRegex.FindAllStringSubmatch(string(bodyBytes), -1)
	discovered := make([]string, 0)
	for _, m := range matches {
		if len(m) > 1 && !strings.Contains(m[1], "Wikipedia") {
			discovered = append(discovered, "https://en.wikipedia.org/wiki/"+m[1])
		}
	}
	return discovered, len(bodyBytes), nil
}
