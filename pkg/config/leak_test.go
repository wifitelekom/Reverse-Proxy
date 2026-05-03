package config

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wraps every test in this package with goleak verification.
// pkg/config has no goroutines of its own, so any leak that fires here
// would necessarily be from a test importing something that does — a
// strong canary for accidental cross-package coupling.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
