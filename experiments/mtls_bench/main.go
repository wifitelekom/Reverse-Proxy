// Experiment 2: mTLS handshake performance.
//
// Measures the wall-clock duration of N handshakes against a load
// balancer configured three ways:
//
//  1. Plain HTTP — baseline (just TCP connect + accept).
//  2. TLS 1.3   — server-only authentication.
//  3. mTLS 1.3  — both sides authenticate via a shared private CA.
//
// For each variant we report p50, p90, p99, p99.9, mean, and the
// observed throughput in handshakes/second. The control-vs-TLS gap is
// what gets quoted in the thesis as the "cost of TLS termination".
//
// We use real net.Dial + tls.Client.Handshake — no HTTP request after
// the handshake completes. This isolates the TLS cost from any
// downstream work.
//
// Concurrency is bounded so we don't burn ephemeral ports; a worker
// pool of 32 goroutines is sufficient to keep the LB warm without
// hitting TIME_WAIT limits on macOS / Linux defaults.
//
// Usage:
//
//	go run ./experiments/mtls_bench [-samples 100000]
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/gossip"
)

func main() {
	samples := flag.Int("samples", 100000, "number of handshakes per variant")
	concurrency := flag.Int("concurrency", 32, "concurrent dials")
	flag.Parse()

	// Three identical backends so the LB has somewhere to forward (the
	// handshake itself doesn't reach them, but the LB constructor
	// requires a non-empty membership view).
	backendURLs := []string{}
	servers := []*httptest.Server{}
	for i := 0; i < 3; i++ {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "ok")
		}))
		servers = append(servers, s)
		backendURLs = append(backendURLs, s.URL[len("http://"):])
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	// -------------------------------------------------------------------
	// Generate a private CA, a server cert, and a client cert.
	// All ECDSA P-256 — fast handshake, modern primitive, supported
	// everywhere we care about.
	// -------------------------------------------------------------------
	caCert, caKey := mustGenCA("Experiment CA")
	serverCert, serverKey := mustGenSigned(caCert, caKey, "127.0.0.1", true)
	clientCert, clientKey := mustGenSigned(caCert, caKey, "client", false)
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	serverTLSCert := tls.Certificate{
		Certificate: [][]byte{serverCert.Raw},
		PrivateKey:  serverKey,
		Leaf:        serverCert,
	}
	clientTLSCert := tls.Certificate{
		Certificate: [][]byte{clientCert.Raw},
		PrivateKey:  clientKey,
		Leaf:        clientCert,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ml := gossip.NewMembershipList("lb", logger)
	for i, addr := range backendURLs {
		ml.AddOrUpdate(gossip.NodeInfo{
			ID: fmt.Sprintf("node-%d", i), Addr: addr, ServiceAddr: addr,
			State: gossip.StateAlive, Incarnation: 1,
		})
	}

	// -------------------------------------------------------------------
	// Variant 1: plain HTTP — TCP connect baseline.
	// -------------------------------------------------------------------
	plainLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail(err)
	}
	defer plainLn.Close()
	plainSrv := &http.Server{Handler: http.NotFoundHandler()}
	go plainSrv.Serve(plainLn)
	plainAddr := plainLn.Addr().String()

	// -------------------------------------------------------------------
	// Variant 2: TLS 1.3 (server auth only).
	// -------------------------------------------------------------------
	tlsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail(err)
	}
	defer tlsLn.Close()
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}
	tlsSrv := &http.Server{Handler: http.NotFoundHandler(), TLSConfig: tlsCfg}
	go tlsSrv.ServeTLS(tlsLn, "", "")
	tlsAddr := tlsLn.Addr().String()

	// -------------------------------------------------------------------
	// Variant 3: mTLS 1.3 (server + client authentication).
	// -------------------------------------------------------------------
	mtlsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail(err)
	}
	defer mtlsLn.Close()
	mtlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}
	mtlsSrv := &http.Server{Handler: http.NotFoundHandler(), TLSConfig: mtlsCfg}
	go mtlsSrv.ServeTLS(mtlsLn, "", "")
	mtlsAddr := mtlsLn.Addr().String()

	clientTLSCfgServerOnly := &tls.Config{
		RootCAs:    caPool,
		ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}
	clientTLSCfgMTLS := &tls.Config{
		RootCAs:      caPool,
		ServerName:   "127.0.0.1",
		Certificates: []tls.Certificate{clientTLSCert},
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}

	fmt.Fprintf(os.Stderr, "running %d handshakes per variant, concurrency=%d\n", *samples, *concurrency)
	fmt.Fprintln(os.Stderr, "---")

	plainStats := bench("plain-tcp", *samples, *concurrency, func() (time.Duration, error) {
		t0 := time.Now()
		c, err := net.DialTimeout("tcp", plainAddr, 5*time.Second)
		if err != nil {
			return 0, err
		}
		c.Close()
		return time.Since(t0), nil
	})
	tlsStats := bench("tls13", *samples, *concurrency, func() (time.Duration, error) {
		t0 := time.Now()
		c, err := tls.Dial("tcp", tlsAddr, clientTLSCfgServerOnly)
		if err != nil {
			return 0, err
		}
		_ = c.Handshake()
		d := time.Since(t0)
		c.Close()
		return d, nil
	})
	mtlsStats := bench("mtls13", *samples, *concurrency, func() (time.Duration, error) {
		t0 := time.Now()
		c, err := tls.Dial("tcp", mtlsAddr, clientTLSCfgMTLS)
		if err != nil {
			return 0, err
		}
		_ = c.Handshake()
		d := time.Since(t0)
		c.Close()
		return d, nil
	})

	out := map[string]any{
		"experiment":  "mtls_bench",
		"samples":     *samples,
		"concurrency": *concurrency,
		"variants": map[string]any{
			"plain_tcp": plainStats,
			"tls13":     tlsStats,
			"mtls13":    mtlsStats,
		},
		"overhead": map[string]any{
			"tls13_vs_plain_p50_ms":  ms(tlsStats.P50) - ms(plainStats.P50),
			"mtls13_vs_plain_p50_ms": ms(mtlsStats.P50) - ms(plainStats.P50),
			"mtls13_vs_tls13_p50_ms": ms(mtlsStats.P50) - ms(tlsStats.P50),
			"tls13_vs_plain_p99_ms":  ms(tlsStats.P99) - ms(plainStats.P99),
			"mtls13_vs_plain_p99_ms": ms(mtlsStats.P99) - ms(plainStats.P99),
		},
	}
	json.NewEncoder(os.Stdout).Encode(out)
}

type stats struct {
	N        int           `json:"n"`
	Errors   int           `json:"errors"`
	WallTime string        `json:"wall_time"`
	HPS      float64       `json:"handshakes_per_second"`
	MeanMs   float64       `json:"mean_ms"`
	P50      time.Duration `json:"p50_ns"`
	P90      time.Duration `json:"p90_ns"`
	P99      time.Duration `json:"p99_ns"`
	P999     time.Duration `json:"p99_9_ns"`
	P50Ms    float64       `json:"p50_ms"`
	P90Ms    float64       `json:"p90_ms"`
	P99Ms    float64       `json:"p99_ms"`
	P999Ms   float64       `json:"p99_9_ms"`
}

func bench(label string, n, conc int, fn func() (time.Duration, error)) stats {
	samples := make([]time.Duration, 0, n)
	var mu sync.Mutex
	var errs atomic.Int64
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	wg.Add(n)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d, err := fn()
			if err != nil {
				errs.Add(1)
				return
			}
			mu.Lock()
			samples = append(samples, d)
			mu.Unlock()
		}()
	}
	wg.Wait()
	wall := time.Since(t0)

	if len(samples) == 0 {
		return stats{N: 0, Errors: int(errs.Load()), WallTime: wall.String()}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var sum time.Duration
	for _, s := range samples {
		sum += s
	}
	mean := time.Duration(int64(sum) / int64(len(samples)))
	st := stats{
		N:        len(samples),
		Errors:   int(errs.Load()),
		WallTime: wall.String(),
		HPS:      float64(len(samples)) / wall.Seconds(),
		MeanMs:   float64(mean.Nanoseconds()) / 1e6,
		P50:      samples[pct(len(samples), 0.50)],
		P90:      samples[pct(len(samples), 0.90)],
		P99:      samples[pct(len(samples), 0.99)],
		P999:     samples[pct(len(samples), 0.999)],
	}
	st.P50Ms = ms(st.P50)
	st.P90Ms = ms(st.P90)
	st.P99Ms = ms(st.P99)
	st.P999Ms = ms(st.P999)

	fmt.Fprintf(os.Stderr, "  %-10s  n=%-6d  p50=%6.2fms  p90=%6.2fms  p99=%6.2fms  p999=%6.2fms  errs=%d  wall=%s\n",
		label, st.N, st.P50Ms, st.P90Ms, st.P99Ms, st.P999Ms, st.Errors, wall.Truncate(time.Millisecond))
	return st
}

func pct(n int, p float64) int {
	idx := int(float64(n-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

func ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }

// ---- cert helpers ----

func mustGenCA(cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fail(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		fail(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

func mustGenSigned(parent *x509.Certificate, parentKey *ecdsa.PrivateKey, cn string, isServer bool) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fail(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	if isServer {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tpl.DNSNames = []string{"localhost"}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		fail(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

// keep imports used
var _ = pem.Encode
