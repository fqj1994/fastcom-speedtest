package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const version = "1.0.0"

type options struct {
	download bool
	upload   bool
	parallel bool

	family  family
	conns   int
	stop    stopConfig
	durDown time.Duration
	durUp   time.Duration

	payload int
	warmup  time.Duration

	latency bool
	probes  int

	json     bool
	quiet    bool
	verbose  bool
	progress bool

	token string
	api   string
	proxy string
}

func main() {
	o, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	os.Exit(run(o))
}

func run(o *options) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stdout, stderr := os.Stdout, os.Stderr
	human := !o.json                // print human-readable results
	showHeader := human && !o.quiet // banner, targets, notes
	liveProgress := showHeader && o.progress && isTerminal(stderr)

	urlCount := clamp(o.conns, 3, 8)
	apiClient := &http.Client{
		Transport: newTransport(o.family.network(), 8, false, o.proxy),
		Timeout:   20 * time.Second,
	}

	token := o.token
	if token == "auto" {
		t, err := refreshToken(ctx, apiClient)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		token = t
	}
	if token == "" {
		token = defaultToken
	}

	info, targets, err := fetchTargets(ctx, apiClient, o.api, token, urlCount)
	if err != nil && o.token == "" {
		// Only the built-in token is allowed to self-heal; an explicit --token
		// that fails is reported rather than silently replaced.
		if t, derr := refreshToken(ctx, apiClient); derr == nil && t != token {
			if i2, t2, e2 := fetchTargets(ctx, apiClient, o.api, t, urlCount); e2 == nil {
				info, targets, err, token = i2, t2, nil, t
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}

	// Data client is pinned to the requested family and HTTP/1.1 so that every
	// in-flight request owns a distinct TCP connection.
	dataClient := &http.Client{Transport: newTransport(o.family.network(), o.conns+2, true, o.proxy)}

	actual := o.family
	if actual == familyAuto {
		actual = family4
		if info.ipv6() {
			actual = family6
		}
	}

	if showHeader {
		printHeader(stdout, info, actual, o.conns, targets, o.verbose, o.parallel)
	}

	report := &jsonReport{
		Version:       version,
		Timestamp:     time.Now().Format(time.RFC3339),
		API:           o.api,
		Parallel:      o.parallel,
		IPFamily:      actual.String(),
		ClientIP:      info.IP,
		ISP:           info.ISP,
		ASN:           info.ASN,
		ClientCity:    info.Location.City,
		ClientCountry: info.Location.Country,
		Connections:   o.conns,
		Targets:       len(targets),
		TargetCities:  uniqueCities(targets),
	}

	if o.latency {
		samples, err := unloadedLatency(ctx, dataClient, targets[0].URL, o.probes)
		if err == nil {
			st := summarizeLatency(samples)
			report.Latency = toJSONLatency(st)
			if human {
				printLatencyLine(stdout, "Latency", st, false)
			}
		} else if showHeader {
			fmt.Fprintf(stderr, "  latency probe failed: %v\n", err)
		}
	}

	ranParallel := false
	if o.parallel && o.download && o.upload && ctx.Err() == nil {
		ranParallel = true
		// Split the connection budget between the two directions so -c still
		// means "total parallel connections".
		dlConns := (o.conns + 1) / 2
		upConns := o.conns / 2

		dlCfg, upCfg := o.stop, o.stop
		dlCfg.hard, upCfg.hard = o.durDown, o.durUp
		if dlCfg.mode == stopStable {
			dlCfg.hard, upCfg.hard = o.stop.hard, o.stop.hard
		}

		dlPh := &phase{dir: dirDownload, conns: dlConns, targets: targets, client: dataClient, stop: dlCfg}
		upPh := &phase{
			dir:     dirUpload,
			conns:   upConns,
			targets: targets,
			client:  dataClient,
			payload: makePayload(o.payload),
			stop:    upCfg,
		}
		dual := newDualProgress(stderr, liveProgress,
			newProgress(stderr, false, "Download", dlConns, dlCfg),
			newProgress(stderr, false, "Upload", upConns, upCfg))

		res := runParallel(ctx, dlPh, upPh, dataClient, targets[0].URL, o.warmup, o.latency,
			func(s sampleInfo) { dual.sample(dirDownload, s) },
			func(s sampleInfo) { dual.sample(dirUpload, s) })
		dual.done()

		report.Download = toJSONPhase(res.Download)
		report.Upload = toJSONPhase(res.Upload)
		if human {
			printPhaseLine(stdout, "Download", res.Download)
		}
		if st := summarizeLatency(res.Loaded); st.N > 0 {
			report.LoadedLatency = toJSONLatency(st)
			if human {
				printLatencyLine(stdout, "Latency", st, true)
			}
		}
		if human {
			printPhaseLine(stdout, "Upload", res.Upload)
		}
	}

	if !ranParallel && o.download && ctx.Err() == nil {
		cfg := o.stop
		cfg.hard = o.durDown
		if cfg.mode == stopStable {
			cfg.hard = o.stop.hard // max-duration wins for stability
		}
		ph := &phase{dir: dirDownload, conns: o.conns, targets: targets, client: dataClient, stop: cfg}
		prog := newProgress(stderr, liveProgress, "Download", o.conns, cfg)

		probeCtx, probeCancel := context.WithCancel(ctx)
		probeDone := make(chan struct{})
		var loaded []time.Duration
		if o.latency {
			go func() {
				defer close(probeDone)
				loaded = loadedLatency(probeCtx, dataClient, targets[0].URL, time.Second)
			}()
		} else {
			close(probeDone)
		}

		res := runPhase(ctx, ph, o.warmup, prog.sample)
		probeCancel()
		<-probeDone
		prog.done()

		report.Download = toJSONPhase(res)
		if human {
			printPhaseLine(stdout, "Download", res)
		}
		if st := summarizeLatency(loaded); st.N > 0 {
			report.LoadedLatency = toJSONLatency(st)
			if human {
				printLatencyLine(stdout, "Latency", st, true)
			}
		}
	}

	if !ranParallel && o.upload && ctx.Err() == nil {
		cfg := o.stop
		cfg.hard = o.durUp
		if cfg.mode == stopStable {
			cfg.hard = o.stop.hard
		}
		ph := &phase{
			dir:     dirUpload,
			conns:   o.conns,
			targets: targets,
			client:  dataClient,
			payload: makePayload(o.payload),
			stop:    cfg,
		}
		prog := newProgress(stderr, liveProgress, "Upload", o.conns, cfg)
		res := runPhase(ctx, ph, o.warmup, prog.sample)
		prog.done()

		report.Upload = toJSONPhase(res)
		if human {
			printPhaseLine(stdout, "Upload", res)
		}
	}

	if showHeader && ctx.Err() != nil {
		fmt.Fprintln(stdout, "\n  interrupted — results above are partial")
	}

	if o.json {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
	}
	return 0
}

// makePayload fills an upload buffer with noise so the body is not a long run
// of zeros (and cannot be collapsed by any middlebox).
func makePayload(n int) []byte {
	if n < 1 {
		n = 1
	}
	b := make([]byte, n)
	var x uint32 = 0x9e3779b9
	for i := 0; i+4 <= n; i += 4 {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i], b[i+1], b[i+2], b[i+3] = byte(x), byte(x>>8), byte(x>>16), byte(x>>24)
	}
	return b
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

const usage = `fast.com speedtest — download/upload against Netflix Open Connect servers.

Usage:
  fastcom-speedtest [options]

Run modes (pick one; default is --duration 10s):
  --duration D            run each phase for a fixed time (default 10s)
  --forever               run until interrupted (Ctrl-C)
  --stable                run until throughput stabilises, then stop

Direction (default: both):
  -d, --download          run only the download test
  -u, --upload            run only the upload test
  -p, --parallel          run download and upload simultaneously, splitting
                          -c between them (requires -c 2 or more)

Network:
  -4, --ipv4              force IPv4
  -6, --ipv6              force IPv6
  -c, --connections N     parallel connections (default 4)
      --payload BYTES     upload body size, max 26214400 (default 26214400)
      --proxy URL         route traffic through a proxy

Stability tuning (--stable):
      --min-duration D          never stop before D (default 7s)
      --max-duration D          always stop after D (default 30s)
      --stability-delta PCT     width of the throughput band (default 2)
      --stable-measurements N   consecutive 1s buckets inside the band (default 6)

Measurement:
      --warmup D                ramp-up excluded from the result (default 1s)
      --download-duration D     per-phase override for --duration
      --upload-duration D
      --no-latency              skip latency probes
      --latency-probes N        unloaded probe count (default 5)

Output:
      --json                  machine-readable output on stdout
  -q, --quiet                 print result lines only
  -v, --verbose               list the target servers
      --no-progress           disable the live progress line

Advanced:
      --token TOKEN           override the app token, or "auto" to scrape it
      --api URL               override the API endpoint
  -V, --version               print the version and exit
  -h, --help                  show this help

Examples:
  fastcom-speedtest -4 -c 8                 IPv4, 8 connections, 10s per phase
  fastcom-speedtest --stable                stop as soon as the rate settles
  fastcom-speedtest --forever -6 -c 16      IPv6, run until Ctrl-C
  fastcom-speedtest -u --duration 20s       upload only, 20s
  fastcom-speedtest -p -c 8                 download+upload at the same time
  fastcom-speedtest --json | jq .download.mbps
`

func parseFlags(args []string) (*options, error) {
	o := &options{api: defaultAPIBase}
	fs := flag.NewFlagSet("fastcom-speedtest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	var (
		dl, ul, ipv4, ipv6 bool
		forever, stable    bool
		showVersion        bool
		dur, durD, durU    time.Duration
		noLatency          bool
		noProgress         bool
	)
	fs.BoolVar(&showVersion, "V", false, "")
	fs.BoolVar(&showVersion, "version", false, "")
	fs.BoolVar(&dl, "d", false, "")
	fs.BoolVar(&dl, "download", false, "")
	fs.BoolVar(&ul, "u", false, "")
	fs.BoolVar(&ul, "upload", false, "")
	fs.BoolVar(&o.parallel, "p", false, "")
	fs.BoolVar(&o.parallel, "parallel", false, "")
	fs.BoolVar(&ipv4, "4", false, "")
	fs.BoolVar(&ipv4, "ipv4", false, "")
	fs.BoolVar(&ipv6, "6", false, "")
	fs.BoolVar(&ipv6, "ipv6", false, "")
	fs.BoolVar(&forever, "forever", false, "")
	fs.BoolVar(&stable, "stable", false, "")

	fs.IntVar(&o.conns, "c", 4, "")
	fs.IntVar(&o.conns, "connections", 4, "")
	fs.DurationVar(&dur, "duration", 10*time.Second, "")
	fs.DurationVar(&durD, "download-duration", 0, "")
	fs.DurationVar(&durU, "upload-duration", 0, "")
	fs.DurationVar(&o.stop.minRun, "min-duration", 7*time.Second, "")
	fs.DurationVar(&o.stop.hard, "max-duration", 30*time.Second, "")
	fs.Float64Var(&o.stop.delta, "stability-delta", 2, "")
	fs.IntVar(&o.stop.stableN, "stable-measurements", 6, "")

	fs.DurationVar(&o.warmup, "warmup", time.Second, "")
	fs.IntVar(&o.payload, "payload", payloadBytes, "")
	fs.IntVar(&o.probes, "latency-probes", 5, "")
	fs.BoolVar(&noLatency, "no-latency", false, "")
	fs.BoolVar(&noProgress, "no-progress", false, "")

	fs.BoolVar(&o.json, "json", false, "")
	fs.BoolVar(&o.quiet, "q", false, "")
	fs.BoolVar(&o.quiet, "quiet", false, "")
	fs.BoolVar(&o.verbose, "v", false, "")
	fs.BoolVar(&o.verbose, "verbose", false, "")

	fs.StringVar(&o.token, "token", "", "")
	fs.StringVar(&o.api, "api", defaultAPIBase, "")
	fs.StringVar(&o.proxy, "proxy", "", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if showVersion {
		fmt.Printf("fastcom-speedtest %s\n", version)
		return nil, flag.ErrHelp
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	// Direction: -d/-u select explicit directions; neither means both.
	if set["d"] || set["download"] || set["u"] || set["upload"] {
		o.download = set["d"] || set["download"]
		o.upload = set["u"] || set["upload"]
	} else {
		o.download, o.upload = true, true
	}

	if ipv4 && ipv6 {
		return nil, fmt.Errorf("--ipv4 and --ipv6 are mutually exclusive")
	}
	switch {
	case ipv4:
		o.family = family4
	case ipv6:
		o.family = family6
	default:
		o.family = familyAuto
	}

	// Run mode: exactly one of duration/forever/stable.
	modes := 0
	for _, on := range []bool{
		set["duration"] || set["download-duration"] || set["upload-duration"],
		forever,
		stable,
	} {
		if on {
			modes++
		}
	}
	if modes > 1 {
		return nil, fmt.Errorf("choose one run mode: --duration, --forever or --stable")
	}
	switch {
	case forever:
		o.stop.mode = stopNever
	case stable:
		o.stop.mode = stopStable
	default:
		o.stop.mode = stopAfter
	}

	// --duration sets both phases; per-phase flags override it.
	if !set["download-duration"] {
		durD = dur
	}
	if !set["upload-duration"] {
		durU = dur
	}
	o.durDown, o.durUp = durD, durU

	if o.stop.mode == stopStable {
		if o.stop.minRun <= 0 {
			o.stop.minRun = 7 * time.Second
		}
		if o.stop.hard <= o.stop.minRun {
			adjusted := o.stop.minRun + 5*time.Second
			fmt.Fprintf(os.Stderr, "note: --max-duration %v is not above --min-duration %v; using %v\n",
				o.stop.hard, o.stop.minRun, adjusted)
			o.stop.hard = adjusted
		}
		if o.stop.stableN < 2 {
			return nil, fmt.Errorf("--stable-measurements must be at least 2")
		}
		if o.stop.delta <= 0 {
			return nil, fmt.Errorf("--stability-delta must be greater than 0")
		}
	}

	if o.conns < 1 {
		return nil, fmt.Errorf("--connections must be at least 1")
	}
	if o.parallel {
		if !o.download || !o.upload {
			return nil, fmt.Errorf("--parallel drives both directions at once; do not combine it with -d/-u")
		}
		if o.conns < 2 {
			return nil, fmt.Errorf("--parallel splits --connections between directions, so it needs -c 2 or more")
		}
	}
	if o.durDown <= 0 || o.durUp <= 0 {
		if o.stop.mode == stopAfter {
			return nil, fmt.Errorf("--duration must be greater than 0")
		}
	}
	if o.warmup < 0 {
		return nil, fmt.Errorf("--warmup cannot be negative")
	}
	if o.payload < 1 {
		return nil, fmt.Errorf("--payload must be greater than 0")
	}
	if o.payload > payloadBytes {
		fmt.Fprintf(os.Stderr, "note: capping --payload at %d bytes (server limit)\n", payloadBytes)
		o.payload = payloadBytes
	}
	if strings.TrimSpace(o.api) == "" {
		o.api = defaultAPIBase
	}

	o.latency = !noLatency
	o.progress = !noProgress
	return o, nil
}
