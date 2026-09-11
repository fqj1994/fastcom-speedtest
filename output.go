package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------- progress --

// progressLine renders a single self-overwriting status line on stderr.
type progressLine struct {
	w       io.Writer
	enabled bool
	label   string
	conns   int
	total   time.Duration // 0 means unbounded
	minRun  time.Duration // shown in stability mode
}

func newProgress(w io.Writer, enabled bool, label string, conns int, cfg stopConfig) *progressLine {
	p := &progressLine{w: w, enabled: enabled, label: label, conns: conns}
	switch cfg.mode {
	case stopAfter:
		p.total = cfg.hard
	case stopStable:
		p.minRun = cfg.minRun
	}
	return p
}

func (p *progressLine) sample(s sampleInfo) {
	if !p.enabled {
		return
	}
	fmt.Fprintf(p.w, "\r\033[K%s", p.format(s))
}

// format renders one status line, without the leading cursor reset.
func (p *progressLine) format(s sampleInfo) string {
	var right string
	switch {
	case p.total > 0:
		right = bar(s.Elapsed, p.total, 18)
	case p.minRun > 0:
		right = fmt.Sprintf("%5.1fs (min %.0fs)", s.Elapsed.Seconds(), p.minRun.Seconds())
	default:
		right = fmt.Sprintf("%5.1fs", s.Elapsed.Seconds())
	}
	if s.Spread >= 0 {
		right += fmt.Sprintf("  Δ%.2f%%", s.Spread)
	}
	if s.Stable {
		right += "  stable"
	}

	return fmt.Sprintf("%-9s %8.1f Mbps  avg %8.1f  peak %8.1f  %s  %dc",
		p.label, s.Mbps, s.Avg, s.Peak, right, p.conns)
}

func (p *progressLine) done() {
	if p.enabled {
		fmt.Fprint(p.w, "\r\033[K")
	}
}

// dualProgress renders two stacked, self-overwriting status lines so a
// simultaneous download+upload run gets one line per direction. Its child
// progressLines are constructed disabled and used only for formatting.
type dualProgress struct {
	w       io.Writer
	enabled bool
	dl, up  *progressLine

	mu     sync.Mutex
	dlInfo *sampleInfo
	upInfo *sampleInfo
}

func newDualProgress(w io.Writer, enabled bool, dl, up *progressLine) *dualProgress {
	return &dualProgress{w: w, enabled: enabled, dl: dl, up: up}
}

func (d *dualProgress) sample(dir string, s sampleInfo) {
	if !d.enabled {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if dir == dirDownload {
		d.dlInfo = &s
	} else {
		d.upInfo = &s
	}
	// Redraw both rows, then park the cursor back on the first row.
	fmt.Fprintf(d.w, "\r\033[K%s\n\r\033[K%s\033[1A\r",
		d.render(d.dl, d.dlInfo), d.render(d.up, d.upInfo))
}

func (d *dualProgress) render(p *progressLine, info *sampleInfo) string {
	if info == nil {
		return ""
	}
	return p.format(*info)
}

func (d *dualProgress) done() {
	if !d.enabled {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// Clear both rows and leave the cursor below them.
	fmt.Fprint(d.w, "\r\033[K\n\r\033[K\n")
}

func bar(elapsed, total time.Duration, width int) string {
	frac := 0.0
	if total > 0 {
		frac = float64(elapsed) / float64(total)
	}
	if frac > 1 {
		frac = 1
	}
	if frac < 0 {
		frac = 0
	}
	n := int(frac * float64(width))
	return "[" + strings.Repeat("#", n) + strings.Repeat("-", width-n) + "]" +
		fmt.Sprintf(" %4.1f/%4.1fs", elapsed.Seconds(), total.Seconds())
}

// ---------------------------------------------------------------- latency ---

type latencyStats struct {
	Min, Avg, Max, Jitter time.Duration
	N                     int
}

func summarizeLatency(s []time.Duration) latencyStats {
	if len(s) == 0 {
		return latencyStats{}
	}
	var sum, jit time.Duration
	for i, d := range s {
		sum += d
		if i > 0 {
			diff := d - s[i-1]
			if diff < 0 {
				diff = -diff
			}
			jit += diff
		}
	}
	c := append([]time.Duration(nil), s...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })

	st := latencyStats{
		Min: c[0],
		Max: c[len(c)-1],
		Avg: sum / time.Duration(len(s)),
		N:   len(s),
	}
	if len(s) > 1 {
		st.Jitter = jit / time.Duration(len(s)-1)
	}
	return st
}

// ----------------------------------------------------------------- report ---

type jsonLatency struct {
	MinMS    float64 `json:"min_ms"`
	AvgMS    float64 `json:"avg_ms"`
	MaxMS    float64 `json:"max_ms"`
	JitterMS float64 `json:"jitter_ms"`
	Probes   int     `json:"probes"`
}

type jsonPhase struct {
	Direction   string  `json:"direction"`
	StopMode    string  `json:"stop_mode"`
	Stable      bool    `json:"stopped_on_stability,omitempty"`
	Mbps        float64 `json:"mbps"`
	PeakMbps    float64 `json:"peak_mbps"`
	Aggregate   float64 `json:"aggregate_mbps"`
	Bytes       int64   `json:"bytes"`
	Seconds     float64 `json:"seconds"`
	WindowSecs  float64 `json:"measurement_window_seconds"`
	Connections int     `json:"connections"`
	Targets     int     `json:"targets"`
	Errors      int64   `json:"errors"`
}

type jsonReport struct {
	Version       string       `json:"version"`
	Timestamp     string       `json:"timestamp"`
	API           string       `json:"api"`
	Parallel      bool         `json:"parallel,omitempty"`
	IPFamily      string       `json:"ip_family"`
	ClientIP      string       `json:"client_ip"`
	ISP           string       `json:"isp"`
	ASN           string       `json:"asn"`
	ClientCity    string       `json:"client_city"`
	ClientCountry string       `json:"client_country"`
	Connections   int          `json:"connections"`
	Targets       int          `json:"targets"`
	TargetCities  []string     `json:"target_cities"`
	Latency       *jsonLatency `json:"latency,omitempty"`
	LoadedLatency *jsonLatency `json:"loaded_latency,omitempty"`
	Download      *jsonPhase   `json:"download,omitempty"`
	Upload        *jsonPhase   `json:"upload,omitempty"`
}

func toJSONLatency(st latencyStats) *jsonLatency {
	if st.N == 0 {
		return nil
	}
	return &jsonLatency{
		MinMS:    msFloat(st.Min),
		AvgMS:    msFloat(st.Avg),
		MaxMS:    msFloat(st.Max),
		JitterMS: msFloat(st.Jitter),
		Probes:   st.N,
	}
}

func toJSONPhase(r phaseResult) *jsonPhase {
	return &jsonPhase{
		Direction:   r.Direction,
		StopMode:    r.Mode.String(),
		Stable:      r.Stable,
		Mbps:        r.Mbps,
		PeakMbps:    r.PeakMbps,
		Aggregate:   r.Aggregate,
		Bytes:       r.Bytes,
		Seconds:     r.Seconds,
		WindowSecs:  r.Window,
		Connections: r.Conns,
		Targets:     r.Targets,
		Errors:      r.Errors,
	}
}

// ---------------------------------------------------------------- printing --

func printHeader(w io.Writer, info *ClientInfo, fam family, conns int, targets []Target, verbose, parallel bool) {
	fmt.Fprintf(w, "fast.com speedtest\n\n")
	fmt.Fprintf(w, "  %-12s %s (%s)  ·  %s  ·  %s, %s\n", "Client",
		info.ISP, "AS"+info.ASN, info.IP, info.Location.City, info.Location.Country)
	fmt.Fprintf(w, "  %-12s %s\n", "IP family", fam)
	fmt.Fprintf(w, "  %-12s %d", "Connections", conns)
	if parallel {
		fmt.Fprintf(w, "  (%d download / %d upload, simultaneous)", (conns+1)/2, conns/2)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %-12s %d OCA servers", "Targets", len(targets))
	if cities := uniqueCities(targets); len(cities) > 0 {
		fmt.Fprintf(w, "  (%s)", strings.Join(cities, ", "))
	}
	fmt.Fprintln(w)
	if verbose {
		for i, t := range targets {
			fmt.Fprintf(w, "      [%d] %s  %s\n", i+1, hostOf(t.URL), t.Location.City)
		}
		fmt.Fprintln(w)
	}
}

func printLatencyLine(w io.Writer, label string, st latencyStats, loaded bool) {
	if st.N == 0 {
		return
	}
	suffix := ""
	if loaded {
		suffix = "  loaded"
	}
	fmt.Fprintf(w, "  %-12s %7.1f ms   min %.1f   jitter %.1f   %d probes%s\n",
		label, msFloat(st.Avg), msFloat(st.Min), msFloat(st.Jitter), st.N, suffix)
}

func printPhaseLine(w io.Writer, label string, r phaseResult) {
	note := ""
	if r.Stable {
		note = "  (stopped on stability)"
	}
	if r.Errors > 0 {
		note += fmt.Sprintf("  (%d retries)", r.Errors)
	}
	fmt.Fprintf(w, "  %-12s %8.2f Mbps   peak %8.2f   %5.1f s   %8s%s\n",
		label, r.Mbps, r.PeakMbps, r.Seconds, humanBytes(r.Bytes), note)
}

func uniqueCities(targets []Target) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range targets {
		c := t.Location.City
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out
}

func hostOf(rawURL string) string {
	rawURL = strings.TrimPrefix(rawURL, "https://")
	rawURL = strings.TrimPrefix(rawURL, "http://")
	if i := strings.IndexByte(rawURL, '/'); i >= 0 {
		return rawURL[:i]
	}
	return rawURL
}

func msFloat(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func humanBytes(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
