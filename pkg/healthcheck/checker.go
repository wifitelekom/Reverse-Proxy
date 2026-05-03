// Package healthcheck performs application-level (HTTP) liveness probes
// alongside the gossip-level membership check.
//
// Why two layers?
//
// The gossip protocol (SWIM) tells us whether a node's UDP gossip socket
// is reachable. That's a fine proxy for "the process is alive and the
// network is up", but it misses common production failure modes:
//
//   - The HTTP handler is deadlocked while goroutines still answer ping.
//   - A dependency is down (DB, cache) and the service can't actually serve
//     real requests, only its health endpoint.
//   - GC pause or CPU starvation causes long-tail latencies.
//
// Consul models this with both gossip + HTTP/TCP/script checks; we follow
// the same pattern. Gossip identifies coarse-grained "is the host up";
// the HTTP checker confirms "is it actually serving".
//
// The checker is opt-in (Config.Enabled). When disabled it is bypassed
// entirely — the LoadBalancer treats every gossip-alive node as eligible,
// preserving prior behaviour.
package healthcheck

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Status enumerates the possible outcomes of a node's health checks.
type Status int

const (
	// StatusUnknown is the initial state for a freshly-added target. The
	// LoadBalancer treats Unknown as eligible (grace period) so that a
	// newly-joined backend sees traffic before its first check completes —
	// without this, every LB restart would 503 until the first tick.
	StatusUnknown Status = iota

	// StatusHealthy means the most recent check returned 2xx within
	// DegradedLatency. Eligible for traffic.
	StatusHealthy

	// StatusDegraded means the node is responsive but slow (2xx above
	// DegradedLatency) or backpressured (HTTP 429). Still eligible for
	// traffic — a future plan may down-weight these in the selector.
	StatusDegraded

	// StatusUnhealthy means UnhealthyThreshold consecutive checks have
	// failed (5xx, network error, or timeout). Removed from rotation
	// until HealthyThreshold consecutive successes restore it.
	StatusUnhealthy
)

// String returns the lowercase enum label, matching the convention in the
// existing gossip package.
func (s Status) String() string {
	switch s {
	case StatusUnknown:
		return "unknown"
	case StatusHealthy:
		return "healthy"
	case StatusDegraded:
		return "degraded"
	case StatusUnhealthy:
		return "unhealthy"
	default:
		return "invalid"
	}
}

// NodeHealth is the full health record for one backend, returned by
// GetHealth and GetAllHealth. All time fields are wall-clock.
type NodeHealth struct {
	NodeID             string        `json:"node_id"`
	Status             Status        `json:"status"`
	StatusString       string        `json:"status_string"`
	LastCheck          time.Time     `json:"last_check"`
	LastSuccess        time.Time     `json:"last_success,omitempty"`
	ResponseTime       time.Duration `json:"response_time_ns"`
	ConsecutiveFails   int           `json:"consecutive_fails"`
	ConsecutiveSuccess int           `json:"consecutive_success"`
	HTTPStatusCode     int           `json:"http_status_code"`
}

// Config controls the periodic checker.
//
// Threshold semantics:
//   - UnhealthyThreshold consecutive failures move Healthy/Unknown to
//     Unhealthy. Setting this above 1 absorbs transient blips.
//   - HealthyThreshold consecutive successes move Unhealthy back to Healthy.
//     Setting this above 1 dampens flapping when a node is recovering
//     intermittently.
type Config struct {
	Enabled            bool
	Interval           time.Duration // tick rate, default 5s
	Timeout            time.Duration // per-request HTTP timeout, default 2s
	Path               string        // URL path probed, default /health
	HealthyThreshold   int           // consecutive successes to leave Unhealthy
	UnhealthyThreshold int           // consecutive failures to enter Unhealthy
	DegradedLatency    time.Duration // 2xx above this is Degraded
}

// DefaultConfig returns production-safe defaults. Enabled=false: callers
// must opt in explicitly.
func DefaultConfig() Config {
	return Config{
		Enabled:            false,
		Interval:           5 * time.Second,
		Timeout:            2 * time.Second,
		Path:               "/health",
		HealthyThreshold:   2,
		UnhealthyThreshold: 3,
		DegradedLatency:    1 * time.Second,
	}
}

// Validate returns an error if the config has illegal values.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Interval <= 0 {
		return fmt.Errorf("health-check interval must be > 0")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("health-check timeout must be > 0")
	}
	if c.Timeout >= c.Interval {
		return fmt.Errorf("health-check timeout (%v) must be < interval (%v)", c.Timeout, c.Interval)
	}
	if c.UnhealthyThreshold <= 0 {
		return fmt.Errorf("unhealthy_threshold must be > 0")
	}
	if c.HealthyThreshold <= 0 {
		return fmt.Errorf("healthy_threshold must be > 0")
	}
	if c.Path == "" {
		return fmt.Errorf("health-check path must not be empty")
	}
	return nil
}

// StatusChangeFunc is invoked, asynchronously, whenever a node's status
// transitions. Use it for logging, metrics, or downstream notifications.
type StatusChangeFunc func(nodeID string, old, new Status)

// MetricsHook is the optional sink for per-probe observations. Defining
// the interface here (rather than importing the metrics package) keeps
// the health-checker self-contained and dependency-light. The hook is
// called from the per-tick goroutines and must therefore be safe for
// concurrent use — Prometheus collectors are.
type MetricsHook interface {
	OnCheck(result string, elapsed time.Duration)
}

// noopHook is the default MetricsHook so the hot path can call through
// without nil checks.
type noopHook struct{}

func (noopHook) OnCheck(string, time.Duration) {}

// Checker periodically probes a configurable set of HTTP endpoints and
// classifies each as Healthy / Degraded / Unhealthy / Unknown.
type Checker struct {
	config Config
	client *http.Client
	logger *slog.Logger

	mu      sync.RWMutex
	nodes   map[string]*NodeHealth
	targets map[string]string // nodeID → "host:port"

	onChange StatusChangeFunc
	hook     MetricsHook
}

// NewChecker returns a Checker with the given config. The HTTP client is
// pre-built with the configured Timeout; callers don't need to bring one.
//
// Note: a zero/disabled config is acceptable — the resulting Checker will
// answer IsHealthy(true) for every node and Start is a no-op.
func NewChecker(cfg Config, logger *slog.Logger) *Checker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Checker{
		config: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
			// Treat redirects as failures: a healthy backend should not
			// be redirecting its own /health endpoint.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger:  logger,
		nodes:   make(map[string]*NodeHealth),
		targets: make(map[string]string),
		hook:    noopHook{},
	}
}

// SetMetricsHook installs a per-probe observation sink. Pass nil to
// restore the no-op. The hook is read concurrently from per-tick
// goroutines, so callers should set it once during startup.
func (c *Checker) SetMetricsHook(h MetricsHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h == nil {
		c.hook = noopHook{}
		return
	}
	c.hook = h
}

// SetOnStatusChange registers a callback fired (in a fresh goroutine, lock
// not held) whenever a node's status changes. Pass nil to clear.
func (c *Checker) SetOnStatusChange(fn StatusChangeFunc) {
	c.mu.Lock()
	c.onChange = fn
	c.mu.Unlock()
}

// UpdateTargets replaces the set of monitored nodes in one shot.
// Targets that disappear from the map are forgotten (their NodeHealth is
// dropped). Targets that appear get a fresh Unknown entry.
//
// LoadBalancer calls this from the slow path of getOrSyncBackends — i.e.
// only when the gossip membership has actually changed — so it is cheap
// in steady state.
func (c *Checker) UpdateTargets(targets map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Drop entries that are no longer present.
	for id := range c.targets {
		if _, still := targets[id]; !still {
			delete(c.targets, id)
			delete(c.nodes, id)
		}
	}
	// Add new entries with Unknown initial state.
	for id, addr := range targets {
		c.targets[id] = addr
		if _, ok := c.nodes[id]; !ok {
			c.nodes[id] = &NodeHealth{NodeID: id, Status: StatusUnknown}
		}
	}
}

// IsHealthy reports whether the node should currently receive traffic.
// Disabled checker → always true. Unknown → true (grace period). Degraded →
// true. Unhealthy → false. Anything else (unknown nodeID) → true so the
// LB doesn't accidentally drop traffic when the checker hasn't been told
// about a backend yet.
func (c *Checker) IsHealthy(nodeID string) bool {
	if !c.config.Enabled {
		return true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	h, ok := c.nodes[nodeID]
	if !ok {
		return true
	}
	return h.Status != StatusUnhealthy
}

// GetHealth returns the current record for one node. The returned value
// is a copy — caller may not mutate the live map. ok=false means unknown.
func (c *Checker) GetHealth(nodeID string) (NodeHealth, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	h, ok := c.nodes[nodeID]
	if !ok {
		return NodeHealth{}, false
	}
	out := *h
	out.StatusString = out.Status.String()
	return out, true
}

// GetAllHealth returns a snapshot of every tracked node. The map is freshly
// allocated; callers may modify it.
func (c *Checker) GetAllHealth() map[string]NodeHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]NodeHealth, len(c.nodes))
	for id, h := range c.nodes {
		v := *h
		v.StatusString = v.Status.String()
		out[id] = v
	}
	return out
}

// Start launches the periodic check loop. Returns immediately; the loop
// runs until ctx is canceled. If config.Enabled is false this is a no-op.
//
// Each tick fans out one goroutine per target so a slow backend doesn't
// block checks against the others.
func (c *Checker) Start(ctx context.Context) {
	if !c.config.Enabled {
		return
	}
	go func() {
		ticker := time.NewTicker(c.config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.checkAll(ctx)
			}
		}
	}()
}

// checkAll snapshots the targets under lock, then probes each in parallel.
// Errors from the goroutines are logged inside checkNode.
func (c *Checker) checkAll(ctx context.Context) {
	c.mu.RLock()
	snapshot := make(map[string]string, len(c.targets))
	for id, addr := range c.targets {
		snapshot[id] = addr
	}
	c.mu.RUnlock()

	var wg sync.WaitGroup
	for id, addr := range snapshot {
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			c.checkNode(ctx, id, addr)
		}(id, addr)
	}
	wg.Wait()
}

// checkNode performs one HTTP probe and updates the node's record.
// Status transitions are applied, and onChange fires (in a fresh goroutine)
// if the status actually changed.
func (c *Checker) checkNode(ctx context.Context, nodeID, addr string) {
	url := "http://" + addr + c.config.Path

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		// URL fundamentally unparseable — treat as failure forever, but
		// log once per occurrence (we'll reach this every tick if the
		// addr is broken).
		c.logger.Debug("health-check request build failed", "node", nodeID, "err", err)
		c.recordFailure(nodeID, 0, 0)
		return
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		c.recordFailure(nodeID, elapsed, 0)
		return
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if elapsed > c.config.DegradedLatency {
			c.recordDegraded(nodeID, elapsed, resp.StatusCode)
		} else {
			c.recordSuccess(nodeID, elapsed, resp.StatusCode)
		}
	case resp.StatusCode == 429:
		c.recordDegraded(nodeID, elapsed, resp.StatusCode)
	default:
		// Any other code (4xx, 5xx) counts as a failure.
		c.recordFailure(nodeID, elapsed, resp.StatusCode)
	}
}

// recordSuccess applies a fast 2xx outcome to the node's record.
func (c *Checker) recordSuccess(nodeID string, elapsed time.Duration, code int) {
	c.applyResult(nodeID, elapsed, code, true, false)
	c.hook.OnCheck("healthy", elapsed)
}

// recordDegraded applies a slow 2xx or 429 outcome.
func (c *Checker) recordDegraded(nodeID string, elapsed time.Duration, code int) {
	c.applyResult(nodeID, elapsed, code, false, true)
	c.hook.OnCheck("degraded", elapsed)
}

// recordFailure applies a 5xx, 4xx (non-429), network error, or timeout.
func (c *Checker) recordFailure(nodeID string, elapsed time.Duration, code int) {
	c.applyResult(nodeID, elapsed, code, false, false)
	c.hook.OnCheck("unhealthy", elapsed)
}

// applyResult is the single point that mutates a node's record. It computes
// the new Status from the current one, the outcome, and the threshold
// counters, fires the change callback if needed, and avoids holding the
// lock across the callback.
func (c *Checker) applyResult(nodeID string, elapsed time.Duration, code int, success, degraded bool) {
	c.mu.Lock()
	h, ok := c.nodes[nodeID]
	if !ok {
		// Target was removed between the start of the check and now —
		// drop the result silently.
		c.mu.Unlock()
		return
	}

	old := h.Status
	h.LastCheck = time.Now()
	h.ResponseTime = elapsed
	h.HTTPStatusCode = code

	switch {
	case success:
		h.LastSuccess = h.LastCheck
		h.ConsecutiveFails = 0
		h.ConsecutiveSuccess++
		// Recovery threshold gating: once Unhealthy, only flip back after
		// a sustained streak of successes. This is the anti-flap guard.
		if old == StatusUnhealthy {
			if h.ConsecutiveSuccess >= c.config.HealthyThreshold {
				h.Status = StatusHealthy
			}
		} else {
			h.Status = StatusHealthy
		}

	case degraded:
		// Degraded resets both counters: a slow reply is neither a win
		// nor a loss in our threshold accounting. Repeated Degraded
		// readings keep the node in rotation but flag it.
		h.ConsecutiveFails = 0
		h.ConsecutiveSuccess = 0
		h.Status = StatusDegraded

	default:
		h.ConsecutiveSuccess = 0
		h.ConsecutiveFails++
		if h.ConsecutiveFails >= c.config.UnhealthyThreshold {
			h.Status = StatusUnhealthy
		}
	}

	newStatus := h.Status
	cb := c.onChange
	c.mu.Unlock()

	if cb != nil && old != newStatus {
		// Run callback in a goroutine so a slow listener doesn't stall
		// the next per-tick check.
		go cb(nodeID, old, newStatus)
	}
}
