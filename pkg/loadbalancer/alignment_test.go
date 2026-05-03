package loadbalancer

import (
	"testing"
	"unsafe"
)

// TestBackend_LatencyEWMAAtOffsetZero is a structural contract test.
//
// Why this matters:
//
// On 32-bit ARM and x86, sync/atomic operations on 64-bit values
// (atomic.LoadInt64, atomic.CompareAndSwapInt64, etc.) require the
// address to be 8-byte aligned, otherwise the runtime panics. Go
// guarantees that the first field of a struct allocated through `new`
// or as a struct literal placed on the heap is at least 8-byte
// aligned. So as long as a 64-bit atomic field sits at offset 0, we
// are safe across every supported GOARCH.
//
// The 64-bit fields elsewhere in the struct could in theory rely on
// other fields' sizes for alignment, but that's brittle: any future
// reorder, or any seemingly-innocent insertion of a smaller field
// before LatencyEWMA, would silently corrupt this on the next 32-bit
// build. Pinning offset == 0 makes the requirement self-checking at
// test time on every architecture, including the 64-bit boxes most
// developers use.
//
// If this test fails, do NOT "fix" it by changing the expected offset.
// Move LatencyEWMA back to position zero in the Backend struct.
func TestBackend_LatencyEWMAAtOffsetZero(t *testing.T) {
	var b Backend
	if got := unsafe.Offsetof(b.LatencyEWMA); got != 0 {
		t.Fatalf("Backend.LatencyEWMA must be at offset 0 for atomic int64 alignment "+
			"on 32-bit GOARCH; got offset %d. See the comment on the Backend struct "+
			"for the full rationale.", got)
	}
}

// TestBackend_AtomicSizeAndAlignment double-checks the runtime's view of
// the field's alignment matches what we expect — covers the (paranoid)
// case where unsafe.Offsetof and unsafe.Alignof disagree on weird
// platforms.
func TestBackend_AtomicSizeAndAlignment(t *testing.T) {
	var b Backend
	// Size sanity: int64 should be 8 bytes everywhere.
	if got := unsafe.Sizeof(b.LatencyEWMA); got != 8 {
		t.Errorf("LatencyEWMA size: got %d, want 8", got)
	}
	// Alignof returns the alignment requirement. For int64 this is 8 on
	// 64-bit and either 4 or 8 on 32-bit (depending on platform). The
	// requirement we care about is that *the address we hand to atomic
	// ops* is 8-aligned, which the offset-zero placement guarantees
	// independently of Alignof's reported value.
	if got := unsafe.Alignof(b.LatencyEWMA); got != 8 && got != 4 {
		t.Errorf("LatencyEWMA Alignof unexpectedly %d", got)
	}
}
