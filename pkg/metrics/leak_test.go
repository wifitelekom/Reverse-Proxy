package metrics

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces zero goroutine leaks for the metrics package. The
// Prometheus collectors we register are pull-based (no background
// goroutines), so this is a tight contract — any future addition that
// spawns its own goroutine will trip this immediately.
//
// Note: prometheus/client_golang's process & Go collectors run no
// background goroutines either; they sample on Gather().
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
