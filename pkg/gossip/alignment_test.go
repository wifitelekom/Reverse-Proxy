package gossip

import (
	"testing"
	"unsafe"
)

// TestProtocol_SeqCounterAtOffsetZero pins the alignment contract for
// Protocol.seqCounter. The same hazard that motivated the
// pkg/loadbalancer Backend.LatencyEWMA reorder applies here:
// atomic.AddUint64 (used in nextSeq) requires 8-byte alignment on
// 32-bit ARM and x86. The Go runtime only guarantees that the first
// field of a heap-allocated struct is 8-byte aligned, so a uint64
// placed elsewhere can land at a 4-byte-aligned offset and crash the
// process on its first atomic op.
//
// Keeping seqCounter at offset 0 makes correctness independent of
// the surrounding layout — even when 32-bit pointer sizes shift the
// offsets of subsequent fields.
//
// If this test fails, do NOT change the expected offset. Move
// seqCounter back to position zero in the Protocol struct definition.
func TestProtocol_SeqCounterAtOffsetZero(t *testing.T) {
	var p Protocol
	if got := unsafe.Offsetof(p.seqCounter); got != 0 {
		t.Fatalf("Protocol.seqCounter must be at offset 0 for atomic uint64 "+
			"alignment on 32-bit GOARCH; got offset %d. See the comment on "+
			"the Protocol struct for the full rationale.", got)
	}
}

// TestProtocol_SeqCounterAlignment cross-checks the runtime's view of
// the alignment requirement. Defensive against the rare case where
// unsafe.Offsetof and unsafe.Alignof disagree on weird platforms.
func TestProtocol_SeqCounterAlignment(t *testing.T) {
	var p Protocol
	if got := unsafe.Sizeof(p.seqCounter); got != 8 {
		t.Errorf("seqCounter size: got %d, want 8", got)
	}
	if got := unsafe.Alignof(p.seqCounter); got != 8 && got != 4 {
		t.Errorf("seqCounter Alignof unexpectedly %d", got)
	}
}
