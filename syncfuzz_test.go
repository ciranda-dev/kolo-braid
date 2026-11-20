package kolobraid

import (
	"bytes"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
)

// FuzzDecodeServed: the stored entries of other histories decode without a panic, and what decodes encodes back to the input.
func FuzzDecodeServed(f *testing.F) {
	k, v, _ := sampleServed(f)
	f.Add(k, v)
	f.Add(k, v[:len(v)/2])
	f.Add(k[:20], v)
	f.Fuzz(func(t *testing.T, k, v []byte) {
		e, err := decodeServed(k, v)
		if err != nil {
			return
		}
		if !bytes.Equal(e.encode(), v) {
			t.Fatal("a served entry decoded from bytes that are not its encoding")
		}
	})
}

// FuzzDecodeCursor: KeysFrom cursors decode without a panic, canonically.
func FuzzDecodeCursor(f *testing.F) {
	var e kolocodec.Encoder
	e.PutUvarint(1 << 40)
	f.Add(e.Bytes())
	f.Add([]byte{0x80, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := decodeCursor(b)
		if err != nil {
			return
		}
		var e kolocodec.Encoder
		e.PutUvarint(c)
		if !bytes.Equal(e.Bytes(), b) {
			t.Fatal("a cursor decoded from bytes that are not its encoding")
		}
	})
}

// FuzzDecodeWelcomeRec: the stored welcome check decodes without a panic, canonically.
func FuzzDecodeWelcomeRec(f *testing.F) {
	f.Add(welcomeRec{Index: 300, ID: EntryID{1}}.encode())
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, b []byte) {
		w, err := decodeWelcomeRec(b)
		if err != nil {
			return
		}
		if !bytes.Equal(w.encode(), b) {
			t.Fatal("a welcome record decoded from bytes that are not its encoding")
		}
	})
}
