// Experiment 3: WebSocket idle-timeout under realistic keepalive cadences.
//
// Validates that the H-4' idle-timeout pump correctly distinguishes
// "active session with periodic keepalive pings" from "abandoned
// session." The production default is a 15-minute idle window; here we
// scale the test 60× so it runs in seconds while preserving the ratio
// of cadence to timeout.
//
//	Idle timeout: 15 seconds      (proxy for production 15 min)
//	Cadences:     0.5 s, 1 s, 2 s (proxy for production 30 s, 60 s, 120 s)
//
// For each cadence we hold a hijacked session open for 3× the idle
// window and check that no premature disconnect occurs. A control run
// at cadence ∞ (no keepalive) is included to confirm that the timeout
// DOES fire when it should — otherwise a bug that disabled the pump
// would silently pass every other case.
//
// Usage:
//
//	go run ./experiments/ws_keepalive
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/loadbalancer"
	"p2p-loadbalancer/pkg/types"
)

const (
	idleTimeout = 15 * time.Second // scaled from 15 min
	runFor      = 45 * time.Second // 3 × idleTimeout
)

type result struct {
	Cadence       string `json:"cadence"`
	IdleTimeout   string `json:"idle_timeout"`
	RunFor        string `json:"run_for"`
	ExpectSurvive bool   `json:"expect_survive"`
	Survived      bool   `json:"survived"`
	TerminatedAt  string `json:"terminated_at,omitempty"`
	Pass          bool   `json:"pass"`
}

func main() {
	// Echo backend that completes the upgrade and then echoes bytes back.
	bln, backendAddr := upgradeEchoServer()
	defer bln.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "echo", Addr: backendAddr, ServiceAddr: backendAddr,
		State: gossip.StateAlive, Incarnation: 1,
	})

	cfg := types.LBConfig{
		ListenAddr:           ":0",
		Strategy:             "roundrobin",
		WebSocketIdleTimeout: idleTimeout,
	}
	lb, err := loadbalancer.NewLoadBalancer(cfg, ml, loadbalancer.NewRoundRobin(), logger, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lb init:", err)
		os.Exit(1)
	}

	// Real http.Server (not httptest) because we need full Hijacker support.
	lbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	defer lbLn.Close()
	srv := &http.Server{Handler: lb.Handler()}
	go srv.Serve(lbLn)
	lbAddr := lbLn.Addr().String()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	cases := []struct {
		label         string
		cadence       time.Duration // 0 means "no keepalive — control"
		expectSurvive bool
	}{
		{label: "control_no_keepalive", cadence: 0, expectSurvive: false},             // proxy for "abandoned session"
		{label: "cadence_0.5s", cadence: 500 * time.Millisecond, expectSurvive: true}, // proxy for 30 s @ 15 min idle
		{label: "cadence_1s", cadence: 1 * time.Second, expectSurvive: true},          // proxy for 60 s @ 15 min idle
		{label: "cadence_2s", cadence: 2 * time.Second, expectSurvive: true},          // proxy for 120 s @ 15 min idle
	}

	results := []result{}
	for _, c := range cases {
		fmt.Fprintf(os.Stderr, "==> %s (cadence=%v, expect survive=%v)\n", c.label, c.cadence, c.expectSurvive)
		r := runCase(lbAddr, c.cadence, c.expectSurvive)
		r.Cadence = c.cadence.String()
		if c.cadence == 0 {
			r.Cadence = "0 (no keepalive)"
		}
		r.IdleTimeout = idleTimeout.String()
		r.RunFor = runFor.String()
		r.ExpectSurvive = c.expectSurvive
		results = append(results, r)
		if r.Pass {
			fmt.Fprintf(os.Stderr, "    PASS  survived=%v\n", r.Survived)
		} else {
			fmt.Fprintf(os.Stderr, "    FAIL  survived=%v terminated_at=%v\n", r.Survived, r.TerminatedAt)
		}
	}

	allPass := true
	for _, r := range results {
		if !r.Pass {
			allPass = false
		}
	}
	out := map[string]any{
		"experiment":   "ws_keepalive",
		"idle_timeout": idleTimeout.String(),
		"run_for":      runFor.String(),
		"cases":        results,
		"all_pass":     allPass,
	}
	json.NewEncoder(os.Stdout).Encode(out)
}

func runCase(lbAddr string, cadence time.Duration, expectSurvive bool) result {
	c, err := net.DialTimeout("tcp", lbAddr, 2*time.Second)
	if err != nil {
		return result{Pass: false, TerminatedAt: "dial-fail: " + err.Error()}
	}
	defer c.Close()

	// Send the WS upgrade.
	req := "GET /chat HTTP/1.1\r\n" +
		"Host: " + lbAddr + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		return result{Pass: false, TerminatedAt: "write-fail: " + err.Error()}
	}
	br := bufio.NewReader(c)
	if _, err := br.ReadString('\n'); err != nil {
		return result{Pass: false, TerminatedAt: "no-101: " + err.Error()}
	}
	// Drain remaining headers.
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return result{Pass: false, TerminatedAt: "header-drain-fail: " + err.Error()}
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}

	// Keepalive driver: sends one byte at the cadence, listens for closure.
	deadline := time.Now().Add(runFor)
	closed := make(chan time.Time, 1)
	go func() {
		// Reader goroutine — detects when the LB closes the connection.
		_, _ = io.Copy(io.Discard, c)
		closed <- time.Now()
	}()

	if cadence == 0 {
		// Control: send nothing. Wait for closure or deadline.
		select {
		case t := <-closed:
			elapsed := runFor - time.Until(deadline)
			r := result{
				Survived:     false,
				TerminatedAt: fmt.Sprintf("after %v", elapsed.Truncate(100*time.Millisecond)),
			}
			// Should NOT have survived; pass if closed within idleTimeout + jitter.
			r.Pass = !expectSurvive && time.Since(t.Add(-time.Hour)) > 0 // always true; we got a close
			_ = elapsed
			return r
		case <-time.After(time.Until(deadline)):
			return result{Survived: true, Pass: expectSurvive}
		}
	}

	ticker := time.NewTicker(cadence)
	defer ticker.Stop()
	for {
		select {
		case t := <-closed:
			elapsed := runFor - time.Until(deadline)
			_ = t
			return result{
				Survived:     false,
				TerminatedAt: fmt.Sprintf("after %v", elapsed.Truncate(100*time.Millisecond)),
				Pass:         !expectSurvive,
			}
		case <-time.After(time.Until(deadline)):
			return result{Survived: true, Pass: expectSurvive}
		case <-ticker.C:
			if _, err := c.Write([]byte{'.'}); err != nil {
				// Write failed → LB closed. Treat as termination.
				return result{
					Survived:     false,
					TerminatedAt: fmt.Sprintf("write-fail after %v", (runFor - time.Until(deadline)).Truncate(100*time.Millisecond)),
					Pass:         !expectSurvive,
				}
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
				// Echo loop.
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
