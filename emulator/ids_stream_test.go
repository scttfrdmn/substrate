package emulator_test

import (
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
)

// TestIDMint_Stream covers a request's side streams: one mint per name for the request, its own
// sequence distinct from the request's, reproducible from the seed, and leaving the request's own
// ordinal where it was.
func TestIDMint_Stream(t *testing.T) {
	t.Parallel()
	m := emulator.NewIDMint("req-stream")
	plain := emulator.NewIDMint("req-stream")

	s := m.Stream("eni")
	assert.Same(t, s, m.Stream("eni"), "the same name is the same stream for the request")
	first := s.Hex(8)
	assert.NotEqual(t, plain.Hex(8), first, "a stream's draws are its own")
	assert.Equal(t, emulator.NewIDMint("req-stream").Stream("eni").Hex(8), first, "and reproducible from the seed")
	assert.Equal(t, plain.Hex(8), func() string { m.Hex(8); return m.Hex(8) }(),
		"a stream's draws do not advance the request's ordinal")
	assert.NotSame(t, s, m.Stream("other"))

	var nilMint *emulator.IDMint
	assert.Nil(t, nilMint.Stream("eni"))
	assert.False(t, emulator.NewIDMint("").Stream("eni").Derived(), "a seedless mint's stream is seedless")
}
