# fastcom-speedtest

A standalone speedtest client for **fast.com / Netflix Open Connect** servers,
written in Go. It talks to the same JSON API the fast.com web app uses, then
runs traffic straight against the returned OCA edge servers.

It supports **download**, **upload**, **multi-connection** tests and explicit
**IPv4 / IPv6** selection, and offers three ways to decide when a test is done.

## Build

```sh
go build -o fastcom-speedtest .
```

No third-party dependencies — standard library only.

## Usage

```
fastcom-speedtest [options]
```

### Run modes (pick one)

| Mode | Flag | Behaviour |
| --- | --- | --- |
| Time | `--duration 10s` *(default)* | Run each phase for a fixed wall-clock time. |
| Forever | `--forever` | Run until interrupted with Ctrl-C; partial results are still reported. |
| Stability | `--stable` | Run until the throughput stops moving, then stop. |

`--stable` is a faithful port of fast.com's own stopper
(`stableDeltaMeasurementsStopper` + `stableMovingAverage(5)`): it keeps testing
until the last *N* aggregate speed measurements all sit inside a *delta*-wide
band around the current smoothed speed, and until the peak is in the past
rather than still climbing. With the defaults (`min 7s`, `max 30s`, `delta 2%`,
`6 measurements`) it typically finishes in 7–9 s, exactly like fast.com.

```sh
--min-duration 7s           never stop before this
--max-duration 30s          always stop after this
--stability-delta 2         width of the band, in percent
--stable-measurements 6     consecutive measurements that must fit the band
```

### Direction

By default both directions run. Passing `-d` and/or `-u` selects explicitly.

```sh
-d, --download      download only
-u, --upload        upload only
-p, --parallel      download and upload at the same time
```

`--parallel` drives both directions concurrently and splits `-c` between them,
so `-c 8` means 4 download + 4 upload connections at once (an odd count gives
the extra to download). It needs `-c 2` or more and cannot be combined with
`-d`/`-u`. Each direction gets its own byte counter, sampler and stop
condition, so under `--stable` they stop independently as each one settles.
Expect lower per-direction numbers than testing them one at a time — that is
the point: it shows how the link behaves under bidirectional load.

### Network

```sh
-4, --ipv4              force IPv4
-6, --ipv6              force IPv6
-c, --connections N     parallel connections (default 4)
    --payload BYTES     upload body size, capped at 26214400 (25 MiB)
    --proxy URL         route traffic through a proxy
```

The API is queried over the selected family, so Netflix returns matching
servers. The data sockets are additionally pinned with `tcp4`/`tcp6` — this
matters because the `ipv6-*.oca.nflxvideo.net` hostnames also publish `A`
records, so name resolution alone would not keep you on IPv6.

HTTP/2 is disabled for the transfer connections on purpose: the OCA servers
speak HTTP/1.1, and multiplexing would silently collapse a multi-connection
test onto a single socket. Each worker is pinned to one server and reuses its
keep-alive connection for the whole phase.

Because `urlCount` is capped at 5 by the API, `-c 16` means roughly three to
four connections per server.

### Measurement & output

```sh
--warmup 1s              exclude TCP slow-start from the result
--download-duration D    per-phase override for --duration
--upload-duration D
--no-latency             skip latency probes
--latency-probes 5       unloaded probe count
--json                   machine-readable output on stdout
-q, --quiet              result lines only
-v, --verbose            list the target servers
--no-progress            disable the live progress line
```

Latency is measured as time-to-first-byte on a 1-byte `Range` request over an
already-established connection: once unloaded, and repeatedly during the
download phase to report the *loaded* latency.

## Examples

```sh
# IPv4, 8 connections, 10 s per phase (default duration)
./fastcom-speedtest -4 -c 8

# IPv6, 16 connections, run until Ctrl-C
./fastcom-speedtest -6 -c 16 --forever

# Stop as soon as the rate settles (fast.com-style adaptive)
./fastcom-speedtest --stable

# Saturate both directions at once (4 download + 4 upload)
./fastcom-speedtest -p -c 8

# Upload only, 20 s, JSON
./fastcom-speedtest -u --duration 20s --json | jq .upload.mbps
```

## Example output

```
fast.com speedtest

  Client       Example ISP (AS64500)  ·  2001:db8::1  ·  Amsterdam, NL
  IP family    IPv6
  Connections  4
  Targets      5 OCA servers  (Amsterdam, Haarlem, Utrecht)
  Latency          4.5 ms   min 4.0   jitter 0.4   5 probes
  Download      4733.61 Mbps   peak  5972.16     8.1 s   17.54 GB  (stopped on stability)
  Latency          3.6 ms   min 3.2   jitter 0.4   8 probes  loaded
  Upload        3699.57 Mbps   peak  6465.67     8.2 s    4.00 GB
```

While a test runs, a live line is drawn on stderr:

```
Download   3589.1 Mbps  avg   3540.2  peak   4514.9    8.1s (min 7s)  Δ2.77%  stable  4c
```

`Δ` is the current deviation from the smoothed speed — the quantity the
stability stopper is watching.

## How it works

1. Fetch targets from `https://api.fast.com/netflix/speedtest/v2`
   (`token`, `urlCount`, `https=true`). If the built-in token is ever rejected,
   the token is re-scraped from fast.com's JS bundle automatically.
2. Each download request returns a fixed 25 MiB chunk; each upload POSTs a
   25 MiB `application/octet-stream` body. Both are looped for the duration of
   the phase against keep-alive connections.
3. Every 150 ms the engine samples the cumulative byte counter, feeds it to the
   fast.com-style aggregator, and evaluates the stop condition.
4. Reported speed is the average over the measurement window (excluding
   `--warmup`), alongside the best sustained 2-second window as `peak`.

## Notes

- `api.fast.com` is queried over the same address family you test with.
- Only the API host and the OCA servers are contacted; nothing else is uploaded.
- If a phase records transient failures they appear as `(N retries)`.
- Not affiliated with Netflix. The token in `api.go` is the public client-side
  token embedded in fast.com's own JavaScript bundle, not a secret, and it is
  re-scraped automatically if Netflix rotates it.
- Intended for personal diagnostics; respect Netflix's terms of service and
  your ISP's acceptable-use policy.
