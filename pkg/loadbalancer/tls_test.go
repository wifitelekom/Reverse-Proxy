package loadbalancer

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

// =====================================================================
// Test helpers
// =====================================================================

// testCert is a self-signed cert/key pair on disk, for use as either a
// server cert or (with isCA=true) a client/backend CA.
type testCert struct {
	CertPath string
	KeyPath  string
	Cert     *x509.Certificate
	Key      *ecdsa.PrivateKey
}

// makeSelfSignedCert generates an ECDSA P-256 self-signed certificate
// covering localhost / 127.0.0.1 / ::1 and writes it to t.TempDir.
//
// ECDSA chosen over RSA for speed: a 2048-bit RSA keygen takes ~50–200ms,
// noticeable across many tests; P-256 is sub-millisecond. Curve selection
// matches what production CAs increasingly use (Let's Encrypt offers ECDSA).
//
// If parent is non-nil, the generated cert is signed by parent (chain CA);
// otherwise self-signed. This lets us build mTLS / backend CA scenarios
// without external tooling.
func makeSelfSignedCert(t *testing.T, parent *testCert, isCA bool) *testCert {
	t.Helper()
	dir := t.TempDir()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	if isCA {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		template.BasicConstraintsValid = true
	}

	parentCert, parentKey := template, any(priv)
	if parent != nil {
		parentCert = parent.Cert
		parentKey = parent.Key
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parentCert, &priv.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	certPem, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("write cert: %v", err)
	}
	pem.Encode(certPem, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	certPem.Close()

	keyDer, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	keyPem, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("write key: %v", err)
	}
	pem.Encode(keyPem, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})
	keyPem.Close()

	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return &testCert{CertPath: certPath, KeyPath: keyPath, Cert: parsed, Key: priv}
}

// =====================================================================
// CertManager
// =====================================================================

func TestCertManager_LoadAndServe(t *testing.T) {
	c := makeSelfSignedCert(t, nil, false)
	cm, err := NewCertManager(c.CertPath, c.KeyPath, discardLogger())
	if err != nil {
		t.Fatalf("NewCertManager: %v", err)
	}
	got, err := cm.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if got == nil || len(got.Certificate) == 0 {
		t.Fatal("empty certificate returned")
	}
}

func TestCertManager_RejectsMissingFiles(t *testing.T) {
	_, err := NewCertManager("/nonexistent/cert.pem", "/nonexistent/key.pem", discardLogger())
	if err == nil {
		t.Fatal("expected error for missing files")
	}
}

func TestCertManager_HotReloadPicksUpNewCert(t *testing.T) {
	// Atomic-store contract: after Reload(), GetCertificate must return
	// the new cert. Verify by checking the serial number changes.
	first := makeSelfSignedCert(t, nil, false)
	cm, err := NewCertManager(first.CertPath, first.KeyPath, discardLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	gotFirst, _ := cm.GetCertificate(nil)
	parsedFirst, _ := x509.ParseCertificate(gotFirst.Certificate[0])

	// Overwrite cert/key files with a new pair (same paths).
	second := makeSelfSignedCert(t, nil, false)
	if err := copyFile(second.CertPath, first.CertPath); err != nil {
		t.Fatalf("overwrite cert: %v", err)
	}
	if err := copyFile(second.KeyPath, first.KeyPath); err != nil {
		t.Fatalf("overwrite key: %v", err)
	}

	if err := cm.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	gotSecond, _ := cm.GetCertificate(nil)
	parsedSecond, _ := x509.ParseCertificate(gotSecond.Certificate[0])

	if parsedFirst.SerialNumber.Cmp(parsedSecond.SerialNumber) == 0 {
		t.Error("serial number unchanged after Reload — old cert still served")
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// =====================================================================
// BuildTLSConfig
// =====================================================================

func TestBuildTLSConfig_Defaults(t *testing.T) {
	c := makeSelfSignedCert(t, nil, false)
	cm, _ := NewCertManager(c.CertPath, c.KeyPath, discardLogger())

	cfg, err := BuildTLSConfig(types.TLSConfig{Enabled: true}, cm)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("default MinVersion: got %x, want TLS12", cfg.MinVersion)
	}
	if cfg.GetCertificate == nil {
		t.Error("GetCertificate not wired")
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Errorf("ClientAuth: got %v, want NoClientCert (mTLS off)", cfg.ClientAuth)
	}
}

func TestBuildTLSConfig_TLS13(t *testing.T) {
	c := makeSelfSignedCert(t, nil, false)
	cm, _ := NewCertManager(c.CertPath, c.KeyPath, discardLogger())

	cfg, err := BuildTLSConfig(types.TLSConfig{Enabled: true, MinVersion: "1.3"}, cm)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion: got %x, want TLS13", cfg.MinVersion)
	}
}

func TestBuildTLSConfig_RejectsUnsupportedVersion(t *testing.T) {
	c := makeSelfSignedCert(t, nil, false)
	cm, _ := NewCertManager(c.CertPath, c.KeyPath, discardLogger())

	_, err := BuildTLSConfig(types.TLSConfig{Enabled: true, MinVersion: "1.0"}, cm)
	if err == nil {
		t.Error("expected error for TLS 1.0")
	}
}

func TestBuildTLSConfig_MTLS(t *testing.T) {
	server := makeSelfSignedCert(t, nil, false)
	ca := makeSelfSignedCert(t, nil, true) // CA cert
	cm, _ := NewCertManager(server.CertPath, server.KeyPath, discardLogger())

	cfg, err := BuildTLSConfig(types.TLSConfig{
		Enabled:      true,
		ClientCAFile: ca.CertPath,
	}, cm)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth: got %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Error("ClientCAs pool empty under mTLS")
	}
}

func TestBuildBackendTLSConfig_Disabled(t *testing.T) {
	cfg, err := BuildBackendTLSConfig(types.BackendTLSConfig{Enabled: false})
	if err != nil {
		t.Fatalf("BuildBackendTLSConfig: %v", err)
	}
	if cfg != nil {
		t.Error("disabled backend TLS should return nil tls.Config")
	}
}

func TestBuildBackendTLSConfig_SkipVerify(t *testing.T) {
	cfg, err := BuildBackendTLSConfig(types.BackendTLSConfig{
		Enabled: true, SkipVerify: true,
	})
	if err != nil {
		t.Fatalf("BuildBackendTLSConfig: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("SkipVerify=true not honored")
	}
}

func TestBuildBackendTLSConfig_CAFile(t *testing.T) {
	ca := makeSelfSignedCert(t, nil, true)
	cfg, err := BuildBackendTLSConfig(types.BackendTLSConfig{
		Enabled: true, CAFile: ca.CertPath,
	})
	if err != nil {
		t.Fatalf("BuildBackendTLSConfig: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs not populated from CAFile")
	}
}

func TestBuildBackendTLSConfig_RejectsMissingCAFile(t *testing.T) {
	_, err := BuildBackendTLSConfig(types.BackendTLSConfig{
		Enabled: true, CAFile: "/nonexistent/ca.pem",
	})
	if err == nil {
		t.Error("expected error for missing CA file")
	}
}

// =====================================================================
// Frontend HTTPS end-to-end
// =====================================================================

func TestHTTPS_FrontendListenerServesTLS(t *testing.T) {
	// Spin up a real HTTPS LB listener on an ephemeral port and confirm
	// a TLS client can complete a handshake and round-trip a request.
	cert := makeSelfSignedCert(t, nil, false)

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)

	cfg := types.LBConfig{
		ListenAddr: "127.0.0.1:0", // resolved at Listen time
		Strategy:   "roundrobin",
		TLS: types.TLSConfig{
			Enabled:    true,
			CertFile:   cert.CertPath,
			KeyFile:    cert.KeyPath,
			MinVersion: "1.2",
		},
	}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	// We can't easily run lb.Start without binding a real socket. Use
	// httptest.NewUnstartedServer so we control the port and TLS config.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(lb.handleRequest))
	srv.TLS = lb.server.TLSConfig
	srv.StartTLS()
	defer srv.Close()

	// The httptest helper also wires up an http.Client that trusts the
	// server's self-signed cert.
	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("HTTPS GET: %v", err)
	}
	defer resp.Body.Close()
	// No backends → 503 expected; the point is that the handshake succeeded.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
	if resp.TLS == nil {
		t.Error("response missing TLS state — handshake didn't happen?")
	}
}

func TestHTTPS_RejectsBelowMinVersion(t *testing.T) {
	// Force the server to TLS 1.3, dial with a 1.2-only client → handshake
	// must fail. This proves MinVersion is actually wired.
	cert := makeSelfSignedCert(t, nil, false)
	cm, _ := NewCertManager(cert.CertPath, cert.KeyPath, discardLogger())
	tlsCfg, err := BuildTLSConfig(types.TLSConfig{Enabled: true, MinVersion: "1.3"}, cm)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.TLS = tlsCfg
	srv.StartTLS()
	defer srv.Close()

	clientCfg := &tls.Config{
		InsecureSkipVerify: true,
		MaxVersion:         tls.VersionTLS12,
	}
	conn, err := tls.Dial("tcp", srv.Listener.Addr().String(), clientCfg)
	if err == nil {
		conn.Close()
		t.Fatal("TLS 1.2 client should not handshake against 1.3-only server")
	}
}

func TestHTTPS_MTLSRequiresClientCert(t *testing.T) {
	// Server requires mTLS. Client without a cert is rejected at handshake.
	server := makeSelfSignedCert(t, nil, false)
	ca := makeSelfSignedCert(t, nil, true)
	cm, _ := NewCertManager(server.CertPath, server.KeyPath, discardLogger())
	tlsCfg, err := BuildTLSConfig(types.TLSConfig{
		Enabled:      true,
		ClientCAFile: ca.CertPath,
	}, cm)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.TLS = tlsCfg
	srv.StartTLS()
	defer srv.Close()

	// No client cert → handshake should fail.
	clientCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.Dial("tcp", srv.Listener.Addr().String(), clientCfg)
	if err == nil {
		// On some Go versions the handshake completes from the client
		// side and the failure surfaces on the first read. Try reading.
		conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
		var buf [1]byte
		_, readErr := conn.Read(buf[:])
		conn.Close()
		if readErr == nil {
			t.Fatal("mTLS server accepted connection without client cert")
		}
	}
}

// =====================================================================
// Backend HTTPS end-to-end
// =====================================================================

func TestBackendTLS_RoutesViaHTTPS(t *testing.T) {
	// httptest.NewTLSServer creates a real HTTPS backend with its own
	// self-signed cert. The LB connects to it with InsecureSkipVerify
	// (since we don't trust the test cert from a fresh CA pool).
	probed := make(chan struct{}, 1)
	httpsBackend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case probed <- struct{}{}:
		default:
		}
		fmt.Fprint(w, "via-tls")
	}))
	defer httpsBackend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	addr := httpsBackend.Listener.Addr().String()
	ml.AddOrUpdate(gossip.NodeInfo{
		ID:          "tls-backend",
		Addr:        addr,
		ServiceAddr: addr,
		State:       gossip.StateAlive,
		Incarnation: 1,
	})

	cfg := types.LBConfig{
		ListenAddr: ":0",
		Strategy:   "roundrobin",
		BackendTLS: types.BackendTLSConfig{Enabled: true, SkipVerify: true},
	}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200 (body=%q)", resp.StatusCode, body)
	}
	if string(body) != "via-tls" {
		t.Errorf("body: got %q, want %q", body, "via-tls")
	}
	select {
	case <-probed:
	case <-time.After(time.Second):
		t.Error("HTTPS backend never received the request")
	}
}

func TestBackendTLS_DisabledUsesHTTP(t *testing.T) {
	// Sanity check: with backend TLS disabled, the LB should NOT try HTTPS.
	// We start a plain HTTP backend and confirm requests succeed — if the LB
	// silently switched to HTTPS, the handshake would fail.
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "via-http")
	}))
	defer plain.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	addr := plain.Listener.Addr().String()
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "plain", Addr: addr, ServiceAddr: addr,
		State: gossip.StateAlive, Incarnation: 1,
	})

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "via-http" {
		t.Errorf("body: %q", body)
	}
}

// =====================================================================
// NewLoadBalancer rejects bad configs
// =====================================================================

func TestNewLoadBalancer_RejectsMissingCertWhenTLSEnabled(t *testing.T) {
	cfg := types.LBConfig{
		ListenAddr: ":0",
		Strategy:   "roundrobin",
		TLS: types.TLSConfig{
			Enabled:  true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		},
	}
	_, err := NewLoadBalancer(cfg, gossip.NewMembershipList("lb", discardLogger()),
		NewRoundRobin(), discardLogger(), nil, nil)
	if err == nil {
		t.Fatal("expected error for missing cert files")
	}
}

func TestNewLoadBalancer_RejectsBadBackendCAFile(t *testing.T) {
	cfg := types.LBConfig{
		ListenAddr: ":0",
		Strategy:   "roundrobin",
		BackendTLS: types.BackendTLSConfig{
			Enabled: true,
			CAFile:  "/nonexistent/ca.pem",
		},
	}
	_, err := NewLoadBalancer(cfg, gossip.NewMembershipList("lb", discardLogger()),
		NewRoundRobin(), discardLogger(), nil, nil)
	if err == nil {
		t.Fatal("expected error for missing backend CA")
	}
}
