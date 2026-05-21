// Experiment 5: Compressed soak with goroutine + heap telemetry.
//
// The full 24-hour soak called for in the third progress report's
// roadmap is impractical inside an interactive thesis run. This
// program substitutes a 5-minute "compressed" soak that exercises the
// same code paths (HTTP request flow, WebSocket session pump, gossip
// background ticks) at sustained load and samples the goroutine count
// and heap residency every 5 seconds. The output is a JSON time series
// suitable for plotting in the thesis evaluation chapter.
//
// The acceptance criterion is *trend, not absolute value*: a healthy
// system shows goroutine and heap counts oscillating in a narrow band
// around their working-set sizes. A leak shows monotonic growth. The
// pre-fix WebSocket pump (audit item H-4', closed during this
// reporting period) would have manifested as a linearly growing
// goroutine count and a linearly growing FD count; this experiment
// confirms the fix by demonstrating flat-trend behaviour.
//
// What's NOT covered:
//   - 24 hours of real-time observation. A production deployment in
//     the operator's own environment is the only honest substitute.
//   - File-descriptor count. macOS's getrlimit-style introspection
//     varies per platform and would add a dependency for marginal
//     additional signal; the goroutine count is a strict proxy for
//     "is the WS pump leaking" since each leaked session leaks a
//     goroutine pair.
//
// Usage:
//
//	go run ./experiments/soak [-duration 5m] [-rps 100]
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/loadbalancer"
	"p2p-loadbalancer/pkg/metrics"
	"p2p-loadbalancer/pkg/types"
)

type sample struct {
	T             float64 `json:"t_sec"`
	Goroutines    int     `json:"goroutines"`
	HeapInUseMB   float64 `json:"heap_inuse_mb"`
	HeapAllocMB   float64 `json:"heap_alloc_mb"`
	NumGC         uint32  `json:"num_gc"`
	RequestsTotal int64   `json:"requests_total"`
}

func main() {
	duration := flag.Duration("duration", 5*time.Minute, "soak duration")
	rps := flag.Int("rps", 100, "target HTTP request rate")
	wsSessions := flag.Int("ws-sessions", 8, "long-lived WebSocket sessions to maintain")
	wsPingMs := flag.Int("ws-ping-ms", 500, "WebSocket keepalive cadence in ms")
	sampleMs := flag.Int("sample-ms", 5000, "telemetry sample interval in ms")
	flag.Parse()

	fmt.Fprintf(os.Stderr, "soak duration=%v  rps=%d  ws_sessions=%d  ws_ping=%dms\n",
		*duration, *rps, *wsSessions, *wsPingMs)

	// Three identical backends — soak focuses on the LB internals, not
	// on the selector.
	backends := []*httptest.Server{}
	for i := 0; i < 3; i++ {
		i := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Millisecond)
			fmt.Fprintf(w, `{"backend":%d}`, i)
		}))
		backends = append(backends, s)
	}
	defer func() {
		for _, s := range backends {
			s.Close()
		}
	}()

	// WebSocket echo backend on a raw TCP listener.
	wsLn, wsAddr := upgradeEchoServer()
	defer wsLn.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ml := gossip.NewMembershipList("lb", logger)
	for i, s := range backends {
		addr := s.URL[len("http://"):]
		ml.AddOrUpdate(gossip.NodeInfo{
			ID:   fmt.Sprintf("node-%d", i),
			Addr: addr, ServiceAddr: addr,
			State: gossip.StateAlive, Incarnation: 1,
		})
	}
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "ws-echo", Addr: wsAddr, ServiceAddr: wsAddr,
		State: gossip.StateAlive, Incarnation: 1,
	})

	cfg := types.LBConfig{
		ListenAddr:           ":0",
		Strategy:             "p2c-ewma",
		WebSocketIdleTimeout: 15 * time.Second, // tight enough to engage during soak
	}
	lb, err := loadbalancer.NewLoadBalancer(cfg, ml, loadbalancer.NewP2CEWMA(), logger, nil, metrics.New())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	lbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer lbLn.Close()
	srv := &http.Server{Handler: lb.Handler()}
	go srv.Serve(lbLn)
	lbAddr := lbLn.Addr().String()
	defer srv.Shutdown(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// HTTP request firehose
	var reqCount atomic.Int64
	client := &http.Client{Timeout: 5 * time.Second}
	httpInterval := time.Second / time.Duration(*rps)
	go func() {
		t := time.NewTicker(httpInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				go func() {
					resp, err := client.Get("http://" + lbAddr + "/")
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					reqCount.Add(1)
				}()
			}
		}
	}()

	// Long-lived WebSocket sessions with periodic pings
	for i := 0; i < *wsSessions; i++ {
		go runWSSession(ctx, lbAddr, time.Duration(*wsPingMs)*time.Millisecond)
	}

	// Telemetry sampler
	samples := []sample{}
	t0 := time.Now()
	ticker := time.NewTicker(time.Duration(*sampleMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			goto report
		case <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			s := sample{
				T:             time.Since(t0).Seconds(),
				Goroutines:    runtime.NumGoroutine(),
				HeapInUseMB:   float64(ms.HeapInuse) / 1024 / 1024,
				HeapAllocMB:   float64(ms.HeapAlloc) / 1024 / 1024,
				NumGC:         ms.NumGC,
				RequestsTotal: reqCount.Load(),
			}
			samples = append(samples, s)
			fmt.Fprintf(os.Stderr, "  t=%5.0fs  goroutines=%-4d  heap_inuse=%5.1fMB  heap_alloc=%5.1fMB  num_gc=%-4d  requests=%d\n",
				s.T, s.Goroutines, s.HeapInUseMB, s.HeapAllocMB, s.NumGC, s.RequestsTotal)
		}
	}

report:
	// Trend analysis: linear regression of goroutines and heap_inuse vs time.
	gSlope, gIntercept := linreg(samples, func(s sample) float64 { return float64(s.Goroutines) })
	hSlope, hIntercept := linreg(samples, func(s sample) float64 { return s.HeapInUseMB })

	// First and last samples for absolute change.
	first, last := samples[0], samples[len(samples)-1]

	out := map[string]any{
		"experiment":      "soak",
		"duration":        duration.String(),
		"target_rps":      *rps,
		"ws_sessions":     *wsSessions,
		"ws_ping_ms":      *wsPingMs,
		"sample_interval": (time.Duration(*sampleMs) * time.Millisecond).String(),
		"samples":         samples,
		"trend": map[string]any{
			"goroutines_slope_per_sec":   gSlope,
			"goroutines_intercept":       gIntercept,
			"goroutines_first":           first.Goroutines,
			"goroutines_last":            last.Goroutines,
			"goroutines_growth_absolute": last.Goroutines - first.Goroutines,
			"heap_mb_slope_per_sec":      hSlope,
			"heap_mb_intercept":          hIntercept,
			"heap_inuse_first_mb":        first.HeapInUseMB,
			"heap_inuse_last_mb":         last.HeapInUseMB,
		},
		"verdict": verdict(gSlope, hSlope),
	}
	json.NewEncoder(os.Stdout).Encode(out)

	fmt.Fprintln(os.Stderr, "---")
	fmt.Fprintf(os.Stderr, "  Goroutines: %d → %d (Δ=%+d). Slope = %+.3f /sec.\n",
		first.Goroutines, last.Goroutines, last.Goroutines-first.Goroutines, gSlope)
	fmt.Fprintf(os.Stderr, "  Heap in use: %.1fMB → %.1fMB. Slope = %+.4f MB/sec.\n",
		first.HeapInUseMB, last.HeapInUseMB, hSlope)
	fmt.Fprintln(os.Stderr, "  Verdict:", verdict(gSlope, hSlope))
}

func runWSSession(ctx context.Context, lbAddr string, ping time.Duration) {
	c, err := net.DialTimeout("tcp", lbAddr, 2*time.Second)
	if err != nil {
		return
	}
	defer c.Close()
	req := "GET /ws HTTP/1.1\r\nHost: " + lbAddr + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		return
	}
	br := bufio.NewReader(c)
	if _, err := br.ReadString('\n'); err != nil {
		return
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	t := time.NewTicker(ping)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.Write([]byte{'.'}); err != nil {
				return
			}
		}
	}
}

func upgradeEchoServer() (net.Listener, string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if strings.TrimSpace(line) == "" {
						break
					}
				}
				_, _ = c.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := br.Read(buf)
					if n > 0 {
						_, _ = c.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln, ln.Addr().String()
}

// linreg returns slope and intercept of y = ax + b through the sample
// series. y is extracted via the accessor.
func linreg(samples []sample, y func(sample) float64) (slope, intercept float64) {
	n := float64(len(samples))
	if n < 2 {
		return 0, 0
	}
	var sx, sy, sxy, sxx float64
	for _, s := range samples {
		x := s.T
		yv := y(s)
		sx += x
		sy += yv
		sxy += x * yv
		sxx += x * x
	}
	d := n*sxx - sx*sx
	if d == 0 {
		return 0, 0
	}
	slope = (n*sxy - sx*sy) / d
	intercept = (sy - slope*sx) / n
	return
}

// verdict converts the regression slopes into a human-readable judgment.
// Threshold values reflect what "flat trend" looks like for a soak:
//
//	|goroutines slope| < 0.05 per second → no leak
//	|heap slope| < 0.01 MB per second   → no leak
func verdict(gSlope, hSlope float64) string {
	gOK := gSlope > -0.05 && gSlope < 0.05
	hOK := hSlope > -0.01 && hSlope < 0.01
	switch {
	case gOK && hOK:
		return "FLAT — no leak observable in goroutine or heap trends."
	case !gOK && hOK:
		return "GOROUTINE TREND — possible goroutine leak; investigate."
	case gOK && !hOK:
		return "HEAP TREND — possible memory growth; investigate."
	default:
		return "BOTH TRENDS — significant growth in goroutines and heap."
	}
}
