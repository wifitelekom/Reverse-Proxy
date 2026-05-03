package gossip

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces zero goroutine leaks across every test in this
// package. Two leak vectors that previously prevented this contract
// have been closed:
//
//   - H-2 (commit C): handlePingReq goroutines now respect ctx and
//     drain on protocol shutdown instead of running until their
//     IndirectTimeout expires.
//
//   - The transport's receiveLoop already exits on ctx.Done; tests
//     that use NewTransport must call Start(ctx) and arrange for
//     the ctx to be canceled before the test returns (typically via
//     t.Cleanup(cancel)).
//
// If a future test forgets cleanup, this guard surfaces the leak
// immediately rather than in some unrelated failure mode weeks later.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
