package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	// defaultAPIBase is the endpoint the fast.com web app calls.
	defaultAPIBase = "https://api.fast.com/netflix/speedtest/v2"

	// defaultToken is the app token currently baked into fast.com's bundle.
	// Netflix rotates it occasionally, so refreshToken() can re-scrape it.
	defaultToken = "YXNkZmFzZGxmbnNkYWZoYXNkZmhrYWxm"

	fastHome = "https://fast.com/"
)

// Location is the city/country block shared by clients and targets.
type Location struct {
	City    string `json:"city"`
	Country string `json:"country"`
}

// Target is one Netflix Open Connect (OCA) speedtest server.
type Target struct {
	Name     string   `json:"name"`
	URL      string   `json:"url"`
	Location Location `json:"location"`
}

// ClientInfo describes how Netflix sees us (and thus which family it served).
type ClientInfo struct {
	IP       string   `json:"ip"`
	ASN      string   `json:"asn"`
	ISP      string   `json:"isp"`
	Location Location `json:"location"`
}

func (c ClientInfo) ipv6() bool { return strings.Contains(c.IP, ":") }

type apiPayload struct {
	Message string     `json:"message"`
	Client  ClientInfo `json:"client"`
	Targets []Target   `json:"targets"`
}

func targetsURL(apiBase, token string, urlCount int) string {
	q := url.Values{}
	q.Set("https", "true")
	q.Set("token", token)
	q.Set("urlCount", strconv.Itoa(urlCount))
	sep := "?"
	if strings.Contains(apiBase, "?") {
		sep = "&"
	}
	return apiBase + sep + q.Encode()
}

// fetchTargets asks the fast.com API for OCA servers. Because Netflix picks
// servers based on the source address of this request, the caller must use a
// client pinned to the desired IP family.
func fetchTargets(ctx context.Context, client *http.Client, apiBase, token string, urlCount int) (*ClientInfo, []Target, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetsURL(apiBase, token, urlCount), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("contacting fast.com API: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("reading fast.com API response: %w", err)
	}

	var p apiPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, nil, fmt.Errorf("fast.com API returned non-JSON (http %d)", resp.StatusCode)
	}
	if len(p.Targets) == 0 {
		if p.Message != "" {
			return nil, nil, fmt.Errorf("fast.com API: %s", p.Message)
		}
		return nil, nil, fmt.Errorf("fast.com API returned no targets (http %d)", resp.StatusCode)
	}
	return &p.Client, p.Targets, nil
}

var (
	appJSRe = regexp.MustCompile(`src="(/app-[^"]*\.js)"`)
	tokenRe = regexp.MustCompile(`token:"([A-Za-z0-9+/=]{16,})"`)
)

// refreshToken re-scrapes the app token out of fast.com's JS bundle so the tool
// keeps working after Netflix rotates the baked-in value.
func refreshToken(ctx context.Context, client *http.Client) (string, error) {
	home, err := httpGet(ctx, client, fastHome, 1<<20)
	if err != nil {
		return "", fmt.Errorf("fetching fast.com: %w", err)
	}
	m := appJSRe.FindSubmatch(home)
	if m == nil {
		return "", fmt.Errorf("could not locate the fast.com app bundle")
	}
	js, err := httpGet(ctx, client, "https://fast.com"+string(m[1]), 8<<20)
	if err != nil {
		return "", fmt.Errorf("fetching %s: %w", m[1], err)
	}
	t := tokenRe.FindSubmatch(js)
	if t == nil {
		return "", fmt.Errorf("no app token found in %s", m[1])
	}
	return string(t[1]), nil
}

func httpGet(ctx context.Context, client *http.Client, rawURL string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, limit))
}
