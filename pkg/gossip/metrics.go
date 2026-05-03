package gossip

// MetricsHook is the optional sink the gossip subsystem reports events to.
// Defining the interface in this package (rather than importing the
// metrics package directly) keeps gossip self-contained — it can be
// embedded in test binaries that don't link Prometheus, and avoids any
// circular-import worry if the metrics package ever needs to refer to
// gossip types.
//
// All methods are called from gossip-internal goroutines. Implementations
// must be safe for concurrent use; a Prometheus collector is.
//
// Strings are used in place of typed enums on purpose: keeping the
// interface dependency-free means the metrics package doesn't have to
// reference gossip.MessageType / gossip.NodeState constants. The cost of
// stringification on the hot path is negligible.
type MetricsHook interface {
	OnMessageSent(msgType string)
	OnMessageReceived(msgType string)
	// OnMessageRejected is called when a message is dropped without
	// being processed. reason is a short, low-cardinality label
	// (e.g. "source_mismatch") that should NOT include nodeID or
	// addresses — those would explode the Prometheus label space.
	OnMessageRejected(reason string)
	OnStateChange(nodeID, from, to string)
	OnLifeguardScore(score int)
}

// noopMetricsHook is the default. Every method is a no-op so the gossip
// hot path can call through unconditionally without nil checks.
type noopMetricsHook struct{}

func (noopMetricsHook) OnMessageSent(string)                 {}
func (noopMetricsHook) OnMessageReceived(string)             {}
func (noopMetricsHook) OnMessageRejected(string)             {}
func (noopMetricsHook) OnStateChange(string, string, string) {}
func (noopMetricsHook) OnLifeguardScore(int)                 {}
