package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"
)

// family selects the IP version used for every socket we open.
type family int

const (
	familyAuto family = iota
	family4
	family6
)

func (f family) String() string {
	switch f {
	case family4:
		return "IPv4"
	case family6:
		return "IPv6"
	default:
		return "auto"
	}
}

// network maps the family onto a net.Dialer network. This is the only reliable
// way to force a version: the OCA hostnames carry both A and AAAA records, so
// an "ipv6-*" name can still resolve and connect over IPv4.
func (f family) network() string {
	switch f {
	case family4:
		return "tcp4"
	case family6:
		return "tcp6"
	default:
		return "tcp"
	}
}

// newTransport builds a transport whose dialer is pinned to netw. When
// http1Only is set, HTTP/2 is disabled so that every in-flight request gets its
// own TCP connection -- otherwise multiplexing would collapse a
// multi-connection test onto a single socket.
func newTransport(netw string, conns int, http1Only bool, proxy string) *http.Transport {
	if conns < 1 {
		conns = 1
	}
	dialer := &net.Dialer{
		Timeout:   15 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, netw, addr)
		},
		MaxIdleConns:          conns * 4,
		MaxIdleConnsPerHost:   conns,
		MaxConnsPerHost:       conns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		ReadBufferSize:        512 << 10,
		WriteBufferSize:       512 << 10,
	}
	if http1Only {
		tr.ForceAttemptHTTP2 = false
		tr.TLSClientConfig = &tls.Config{
			NextProtos: []string{"http/1.1"},
			MinVersion: tls.VersionTLS12,
		}
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	}
	return tr
}
