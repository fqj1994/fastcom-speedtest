package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// payloadBytes is the largest body a fast.com OCA server accepts, and also
	// the exact size of every download chunk it returns.
	payloadBytes = 26214400 // 25 MiB
	copyBufSize  = 512 << 10

	// measureInterval matches fast.com's progressFrequencyMs.
	measureInterval = 150 * time.Millisecond

	// stableAverageWindow mirrors fast.com's stableMovingAverage(5) aggregator.
	stableAverageWindow = 5

	dirDownload = "download"
	dirUpload   = "upload"
)

// stopMode selects how a phase decides it is finished.
type stopMode int

const (
	stopAfter  stopMode = iota // run for a fixed wall-clock duration
	stopNever                  // run until the context is cancelled (Ctrl-C)
	stopStable                 // run until throughput stops moving
)

func (m stopMode) String() string {
	switch m {
	case stopAfter:
		return "duration"
	case stopNever:
		return "forever"
	case stopStable:
		return "stability"
	}
	return "unknown"
}

// stopConfig bundles the knobs for the three run modes.
//
//	stopAfter : hard = phase duration
//	stopNever : everything ignored
//	stopStable: minRun..hard, stopping once the last stableN aggregate speeds
//	            all sit within delta percent of the current speed
type stopConfig struct {
	mode    stopMode
	hard    time.Duration
	minRun  time.Duration
	delta   float64
	stableN int
}

// point is a cumulative byte count at a point in time.
type point struct {
	T     time.Duration
	Bytes int64
}

// snap is one tick's worth of transferred bytes and elapsed time.
type snap struct {
	bytes int64
	secs  float64
}

type phaseResult struct {
	Direction string
	Mode      stopMode
	Stable    bool
	Bytes     int64
	Seconds   float64
	Window    float64 // seconds of data used for the headline Mbps
	Mbps      float64
	PeakMbps  float64
	Aggregate float64 // fast.com-style smoothed speed at the end
	Conns     int
	Targets   int
	Errors    int64
}

// sampleInfo is pushed to the live progress line on every tick.
type sampleInfo struct {
	Elapsed time.Duration
	Bytes   int64
	Mbps    float64 // smoothed aggregate speed
	Avg     float64 // post-warm-up average, i.e. the headline metric
	Peak    float64 // best sustained 2s window so far
	Stable  bool
	Spread  float64 // percent deviation from the aggregate, -1 if unknown
}

type counter struct{ n *atomic.Int64 }

func (c counter) Write(p []byte) (int, error) { c.n.Add(int64(len(p))); return len(p), nil }

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}

type phase struct {
	dir     string
	conns   int
	targets []Target
	client  *http.Client
	payload []byte
	stop    stopConfig
	total   atomic.Int64
	errs    atomic.Int64
}

// runPhase drives conns workers until the stop condition fires and returns the
// measured throughput. onSample (optional) is called on every tick so callers
// can render live progress.
func runPhase(ctx context.Context, p *phase, warmup time.Duration, onSample func(sampleInfo)) phaseResult {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	if p.stop.mode != stopNever && p.stop.hard > 0 {
		timer := time.AfterFunc(p.stop.hard, cancel)
		defer timer.Stop()
	}

	var (
		points       []point
		snaps        []snap
		measurements []float64
		agg          = newAggregator(stableAverageWindow)
		curSpeed     float64
		runPeak      float64
		peakStart    int
		baseBytes    int64
		baseT        time.Duration
		lastBytes    int64
		lastT        time.Duration
		stableHit    bool
	)

	// tick samples the byte counter, updates the fast.com-style aggregate and
	// decides whether the throughput has stabilised. It runs on a single
	// goroutine at a time (the sampler, plus one final call) so it needs no
	// locking; only p.total/p.errs are shared with the workers.
	tick := func() {
		now := time.Since(start)
		cur := p.total.Load()

		dt := (now - lastT).Seconds()
		points = append(points, point{T: now, Bytes: cur})
		snaps = append(snaps, snap{bytes: cur - lastBytes, secs: dt})
		lastBytes, lastT = cur, now

		curSpeed = agg.update(snaps)
		measurements = append(measurements, curSpeed)

		// Best sustained 2s window so far. peakStart/baseBytes mirror the
		// warm-up baseline used by computeStats so the live numbers match the
		// final ones. The window only spans ~2s of samples, so this is O(1).
		if now <= warmup {
			peakStart, baseBytes, baseT = len(points)-1, cur, now
		}
		liveAvg := 0.0
		if dt := (now - baseT).Seconds(); dt > 0 {
			liveAvg = float64(cur-baseBytes) * 8 / dt / 1e6
		}
		if localPeak := windowPeak(points, peakStart, len(points)-1, peakWindowSeconds, minPeakWindowSeconds); now >= warmup && localPeak > runPeak {
			runPeak = localPeak
		}

		if onSample == nil {
			return
		}
		info := sampleInfo{Elapsed: now, Bytes: cur, Mbps: curSpeed, Avg: liveAvg, Peak: runPeak, Spread: -1}
		if len(measurements) >= p.stop.stableN {
			info.Spread = maxDelta(curSpeed, measurements[len(measurements)-p.stop.stableN:])
		}
		if p.stop.mode == stopStable && stopperCompleted(now, curSpeed, measurements, p.stop) {
			info.Stable = true
			stableHit = true
			onSample(info)
			cancel()
			return
		}
		onSample(info)
	}

	var wg sync.WaitGroup
	for i := 0; i < p.conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, copyBufSize)
			// Pin each worker to one server so its TCP/TLS connection is reused
			// for the whole phase instead of being rebuilt per request.
			target := p.targets[id%len(p.targets)].URL
			for runCtx.Err() == nil {
				var err error
				if p.dir == dirUpload {
					err = p.upload(runCtx, target)
				} else {
					err = p.download(runCtx, target, buf)
				}
				if err != nil {
					if runCtx.Err() != nil {
						return
					}
					p.errs.Add(1)
					select {
					case <-time.After(200 * time.Millisecond):
					case <-runCtx.Done():
						return
					}
				}
			}
		}(i)
	}

	samplerDone := make(chan struct{})
	var endAt time.Duration
	go func() {
		defer close(samplerDone)
		t := time.NewTicker(measureInterval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				// Stamp the moment the byte counter stopped moving. Waiting
				// for the workers to unwind would otherwise add dead time
				// that drags the reported average down.
				endAt = time.Since(start)
				return
			case <-t.C:
				tick()
			}
		}
	}()

	wg.Wait()
	cancel()
	<-samplerDone
	if endAt <= 0 {
		endAt = time.Since(start)
	}
	points = append(points, point{T: endAt, Bytes: p.total.Load()})

	res := phaseResult{
		Direction: p.dir,
		Mode:      p.stop.mode,
		Stable:    stableHit,
		Aggregate: curSpeed,
		Conns:     p.conns,
		Targets:   len(p.targets),
		Errors:    p.errs.Load(),
	}
	if len(points) > 0 {
		last := points[len(points)-1]
		res.Bytes = last.Bytes
		res.Seconds = last.T.Seconds()
	}
	base := baselineIndex(points, warmup)
	res.Mbps, res.PeakMbps = computeStats(points, base)
	res.Window = windowSeconds(points, base)
	return res
}

// parallelResult is the outcome of a simultaneous download+upload run.
type parallelResult struct {
	Download phaseResult
	Upload   phaseResult
	Loaded   []time.Duration
}

// runParallel drives a download phase and an upload phase at the same time
// against the same client, so the link is exercised in both directions at once.
// Each phase keeps its own byte counter, sampler and stop condition; the call
// returns once both have finished.
func runParallel(ctx context.Context, dl, up *phase, client *http.Client, probeTarget string, warmup time.Duration, probe bool, dlSink, upSink func(sampleInfo)) parallelResult {
	var (
		wg  sync.WaitGroup
		res parallelResult
	)

	// Loaded latency is probed while the link is busy in both directions.
	probeCtx, probeCancel := context.WithCancel(ctx)
	probeDone := make(chan struct{})
	if probe {
		go func() {
			defer close(probeDone)
			res.Loaded = loadedLatency(probeCtx, client, probeTarget, time.Second)
		}()
	} else {
		close(probeDone)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		res.Download = runPhase(ctx, dl, warmup, dlSink)
	}()
	go func() {
		defer wg.Done()
		res.Upload = runPhase(ctx, up, warmup, upSink)
	}()
	wg.Wait()

	probeCancel()
	<-probeDone
	return res
}

// ------------------------------------------------------- fast.com algorithm --

// aggregator is a port of fast.com's aggregator/stableMovingAverage. It tracks
// a moving average while throughput is still climbing, then freezes and
// reports a cumulative average once it stops climbing -- which yields the
// smooth, slightly-conservative number fast.com displays.
type aggregator struct {
	window   int
	resetThr int

	startInd   int
	curSpeed   float64
	fixedStart bool
	bytes      float64
	times      float64
}

func newAggregator(window int) *aggregator {
	a := &aggregator{window: window, resetThr: 5}
	a.reset()
	return a
}

func (a *aggregator) reset() {
	a.startInd, a.curSpeed, a.bytes, a.times, a.fixedStart = 0, 0, 0, 0, false
}

// update consumes the full snapshot history and returns Mbps.
func (a *aggregator) update(snaps []snap) float64 {
	if len(snaps) < a.resetThr {
		a.reset()
	}
	if !a.fixedStart {
		var sumBytes, sumTime float64
		start := len(snaps) - 1
		end := len(snaps) - a.window
		if end < 0 {
			end = 0
		}
		for i := start; i >= end; i-- {
			sumBytes += float64(snaps[i].bytes)
			sumTime += snaps[i].secs
		}
		speed := 0.0
		if sumTime > 0 {
			speed = sumBytes / sumTime
		}
		if speed >= a.curSpeed {
			a.startInd = start + 1
			a.curSpeed = speed
			a.bytes = sumBytes
			a.times = sumTime
		} else {
			a.fixedStart = true
		}
	}
	for i := a.startInd; i < len(snaps); i++ {
		a.bytes += float64(snaps[i].bytes)
		a.times += snaps[i].secs
	}
	a.startInd = len(snaps)
	if a.times > 0 {
		return a.bytes / a.times * 8 / 1e6
	}
	return 0
}

// stopperCompleted mirrors fast.com's stableDeltaMeasurementsStopper.isCompleted.
// The max-duration cutoff is enforced separately by a timer, so a true return
// always means the throughput genuinely settled.
func stopperCompleted(elapsed time.Duration, curSpeed float64, m []float64, cfg stopConfig) bool {
	if elapsed < cfg.minRun || len(m) < cfg.stableN {
		return false
	}
	// The peak of the last ceil(n/2) measurements must sit at least
	// ceil(n/2) positions from the end, i.e. we are past the peak rather
	// than still ramping up.
	half := (cfg.stableN + 1) / 2
	if len(m)-lastWindowMaxInd(m, half) < half {
		return false
	}
	return maxDelta(curSpeed, m[len(m)-cfg.stableN:]) <= cfg.delta
}

// lastWindowMaxInd returns the index of the highest value in the last
// windowSize entries (earliest wins ties, matching fast.com).
func lastWindowMaxInd(m []float64, windowSize int) int {
	curMax, curMaxInd := 0.0, 0
	first := len(m) - windowSize
	if first < 0 {
		first = 0
	}
	for i := len(m) - 1; i >= first; i-- {
		if m[i] >= curMax {
			curMax, curMaxInd = m[i], i
		}
	}
	return curMaxInd
}

// maxDelta returns the largest percentage deviation from value.
func maxDelta(value float64, m []float64) float64 {
	if value <= 0 {
		return 1e9
	}
	worst := 0.0
	for _, v := range m {
		if d := 100 * abs(v-value) / value; d > worst {
			worst = d
		}
	}
	return worst
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// ------------------------------------------------------------------ transfer --

func (p *phase) download(ctx context.Context, target string, buf []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("download: http %d", resp.StatusCode)
	}
	_, err = io.CopyBuffer(counter{&p.total}, resp.Body, buf)
	return err
}

func (p *phase) upload(ctx context.Context, target string) error {
	body := &countingReader{r: bytes.NewReader(p.payload), n: &p.total}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(p.payload))
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("upload: http %d", resp.StatusCode)
	}
	return nil
}

// ---------------------------------------------------------------- statistics --

// baselineIndex returns the last sample at or before the warm-up boundary, so
// that TCP slow-start does not drag the reported average down.
func baselineIndex(pts []point, warmup time.Duration) int {
	base := 0
	for i, p := range pts {
		if p.T <= warmup {
			base = i
		} else {
			break
		}
	}
	if base >= len(pts)-1 {
		return 0
	}
	return base
}

func windowSeconds(pts []point, base int) float64 {
	if len(pts) < 2 || base >= len(pts)-1 {
		return 0
	}
	return (pts[len(pts)-1].T - pts[base].T).Seconds()
}

// computeStats returns the mean and the best short-window throughput in Mbps.
func computeStats(pts []point, base int) (avg, peak float64) {
	if len(pts) < 2 {
		return 0, 0
	}
	last := pts[len(pts)-1]
	if w := windowSeconds(pts, base); w > 0 {
		avg = float64(last.Bytes-pts[base].Bytes) * 8 / w / 1e6
	}
	peak = avg
	for i := base + 1; i < len(pts); i++ {
		if v := windowPeak(pts, base, i, peakWindowSeconds, minPeakWindowSeconds); v > peak {
			peak = v
		}
	}
	return avg, peak
}

const (
	peakWindowSeconds    = 2
	minPeakWindowSeconds = 1
)

// windowPeak returns the throughput (Mbps) of the longest window of at most
// `window` seconds ending at points[end], provided it covers at least
// `minWindow` seconds -- otherwise the sample is too short to be a meaningful
// sustained rate. Requiring a floor is what keeps a brief burst from being
// reported as the peak.
func windowPeak(pts []point, start, end int, window, minWindow float64) float64 {
	j := end
	for j > start && (pts[end].T-pts[j-1].T).Seconds() <= window {
		j--
	}
	if j == end {
		return 0
	}
	dt := (pts[end].T - pts[j].T).Seconds()
	if dt < minWindow {
		return 0
	}
	return float64(pts[end].Bytes-pts[j].Bytes) * 8 / dt / 1e6
}

// ------------------------------------------------------------------ latency --

// probeOnce measures round-trip time to first byte using a 1-byte range read on
// an already-established keep-alive connection.
func probeOnce(ctx context.Context, client *http.Client, target string) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", "bytes=0-0")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	d := time.Since(start)
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	return d, nil
}

func unloadedLatency(ctx context.Context, client *http.Client, target string, n int) ([]time.Duration, error) {
	if _, err := probeOnce(ctx, client, target); err != nil { // warm the connection
		return nil, err
	}
	out := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		d, err := probeOnce(ctx, client, target)
		if err != nil {
			continue
		}
		out = append(out, d)
		time.Sleep(50 * time.Millisecond)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("all latency probes failed")
	}
	return out, nil
}

// loadedLatency probes while a transfer phase is running; the first probe is
// discarded so connection warm-up does not skew the result.
func loadedLatency(ctx context.Context, client *http.Client, target string, interval time.Duration) []time.Duration {
	if _, err := probeOnce(ctx, client, target); err != nil {
		return nil
	}
	var (
		mu  sync.Mutex
		out []time.Duration
	)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()
			return append([]time.Duration(nil), out...)
		case <-t.C:
			if d, err := probeOnce(ctx, client, target); err == nil {
				mu.Lock()
				out = append(out, d)
				mu.Unlock()
			}
		}
	}
}
