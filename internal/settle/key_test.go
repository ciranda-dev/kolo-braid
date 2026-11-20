package settle

import (
	"encoding/hex"
	"testing"
)

// TestConfigIDVectors: the config ID that readiness pulses name equals the format's known answers, computed by an
// independent program for the root package's ConfigIDOf.
func TestConfigIDVectors(t *testing.T) {
	seat := func(key, strand byte) Seat {
		var s Seat
		for i := range 32 {
			s.Key[i], s.Strand[i] = key+byte(i), strand+byte(i)
		}
		return s
	}
	cases := []struct {
		n     uint64
		rule  Rule
		seats []Seat
		want  string
	}{
		{0, Majority, []Seat{seat(0x40, 0x50)}, "b013a7d6837e5a8c393505b5076a77b8bc6fd2e0919aaca59a536df94c2b459c"},
		{200, TwoThirds, []Seat{seat(0x10, 0x20), seat(0x50, 0x60), seat(0x90, 0xa0)}, "a270154fa22a19a5f4349378c51ed6b2f1f911a04bfbea8a632034cc0236264d"},
		{1, Majority, nil, "66ec80e74d1a64a6ffe7eab970e168561e8f8741fd7e8fdd86fb55bf9d72dc73"},
	}
	for _, c := range cases {
		got := Config{Number: c.n, Rule: c.rule, Seats: c.seats}.ID()
		if hex.EncodeToString(got[:]) != c.want {
			t.Fatalf("config %d: %x, want %s", c.n, got, c.want)
		}
	}
}
