package loadbalancer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/types"
)

// CertManager owns the live X.509 certificate used by the HTTPS listener.
// It supports atomic hot-reload from disk without dropping connections —
// the standard library calls GetCertificate on every handshake, so a new
// pointer in the atomic.Value is picked up immediately.
//
// Why an atomic.Pointer rather than a sync.RWMutex? Handshakes can run on
// every accepted connection; a lock-free read is meaningfully cheaper at
// high request rates. Reload is rare (every hour by default) and a CAS
// is overkill; a plain Store is fine because writes serialize naturally
// through Reload.
type CertManager struct {
	certFile string
	keyFile  string
	current  atomic.Pointer[tls.Certificate]
	logger   *slog.Logger
}

// NewCertManager loads the cert/key once and returns a manager. Returns
// an error immediately if the files don't parse — callers should fail fast
// rather than start an HTTPS listener that can't terminate handshakes.
func NewCertManager(certFile, keyFile string, logger *slog.Logger) (*CertManager, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cm := &CertManager{certFile: certFile, keyFile: keyFile, logger: logger}
	if err := cm.Reload(); err != nil {
		return nil, err
	}
	return cm, nil
}

// Reload re-reads the cert/key from disk and atomically swaps the cached
// certificate. Safe to call from any goroutine. On error the previous
// certificate stays in place — a corrupt file mid-rotation must not break
// existing handshakes.
func (cm *CertManager) Reload() error {
	cert, err := tls.LoadX509KeyPair(cm.certFile, cm.keyFile)
	if err != nil {
		return fmt.Errorf("load cert/key: %w", err)
	}
	cm.current.Store(&cert)
	cm.logger.Info("TLS certificate loaded", "cert_file", cm.certFile)
	return nil
}

// GetCertificate is a tls.Config.GetCertificate callback. The lookup is a
// single atomic load — no allocations, no locks.
func (cm *CertManager) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if c := cm.current.Load(); c != nil {
		return c, nil
	}
	return nil, fmt.Errorf("no certificate loaded")
}

// StartAutoReload runs Reload() on a ticker until ctx is canceled. Errors
// are logged and ignored — a transient read failure should not abort the
// entire LB. interval <= 0 disables the loop (no-op).
func (cm *CertManager) StartAutoReload(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := cm.Reload(); err != nil {
					cm.logger.Warn("TLS auto-reload failed", "err", err)
				}
			}
		}
	}()
}

// BuildTLSConfig assembles a *tls.Config from the supplied TLSConfig and
// CertManager. The returned *tls.Config:
//
//   - Reads its certificate via cm.GetCertificate (so reloads are picked up)
//   - Enforces MinVersion (1.2 or 1.3 only)
//   - Sets ClientAuth + ClientCAs when ClientCAFile is configured (mTLS)
//
// CipherSuites is left at the Go default — the standard library curates a
// reasonable set per Go version, and pinning here would only delay
// security upgrades.
func BuildTLSConfig(cfg types.TLSConfig, cm *CertManager) (*tls.Config, error) {
	min, err := parseTLSVersion(cfg.MinVersion)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion:     min,
		GetCertificate: cm.GetCertificate,
	}
	if cfg.ClientCAFile != "" {
		pool, err := loadCAPool(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("client CA: %w", err)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, nil
}

// BuildBackendTLSConfig builds the *tls.Config used by the LB→backend leg.
// Returns nil if Enabled is false (caller's hint to skip TLS setup).
//
// SkipVerify deliberately surfaces unchanged from the config — we let the
// validator log a warning rather than refuse the configuration, because
// dev/test setups legitimately need this.
func BuildBackendTLSConfig(cfg types.BackendTLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify, //nolint:gosec — operator-controlled
	}
	if cfg.CAFile != "" {
		pool, err := loadCAPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("backend CA: %w", err)
		}
		tlsCfg.RootCAs = pool
	}
	return tlsCfg, nil
}

// parseTLSVersion converts a string ("1.2", "1.3") to the tls package
// constant. Empty defaults to 1.2 for safety.
func parseTLSVersion(s string) (uint16, error) {
	switch s {
	case "", "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unsupported TLS version %q (want 1.2 or 1.3)", s)
	}
}

// loadCAPool reads a PEM bundle from disk and returns an x509 pool with
// it. Empty/unparseable input returns an error rather than an empty pool —
// silently accepting that would be a configuration footgun.
func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no PEM certificates found in %s", path)
	}
	return pool, nil
}
