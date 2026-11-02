package wire_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/sjlit/mux/wire"
)

func FuzzDecodeFrame(f *testing.F) {
	for _, v := range testVectors(f) {
		f.Add(v.raw)
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		fr, n, err := wire.DecodeFrame(b, 32768)
		if err != nil {
			if n != 0 {
				t.Fatalf("DecodeFrame returned n=%d with error %v", n, err)
			}
			return
		}
		if n < wire.HeaderSize || n > len(b) {
			t.Fatalf("DecodeFrame consumed %d of %d bytes", n, len(b))
		}
		if got, want := len(fr.Payload), int(binary.LittleEndian.Uint16(b[2:4])); got != want {
			t.Fatalf("payload length = %d, header says %d", got, want)
		}

		hdr, total, err := wire.DecodeHeader(b[:wire.HeaderSize], 32768)
		if err != nil {
			t.Fatalf("DecodeHeader on valid frame: %v", err)
		}
		if total != n {
			t.Fatalf("DecodeHeader total = %d, DecodeFrame consumed %d", total, n)
		}
		if hdr.Type != fr.Type || hdr.Flags != fr.Flags || hdr.StreamID != fr.StreamID {
			t.Fatalf("DecodeHeader = %+v, DecodeFrame = %+v", hdr, fr)
		}

		encoded, err := wire.AppendFrame(nil, fr)
		if err != nil {
			t.Fatalf("AppendFrame on decoded frame: %v", err)
		}
		got, n2, err := wire.DecodeFrame(encoded, 32768)
		if err != nil {
			t.Fatalf("DecodeFrame on re-encoded frame: %v", err)
		}
		if n2 != len(encoded) {
			t.Fatalf("re-encoded frame consumed %d of %d bytes", n2, len(encoded))
		}
		if !framesEqual(fr, got) {
			t.Fatalf("round trip = %+v, want %+v", got, fr)
		}

		for _, k := range prefixLengths(n) {
			if k >= n {
				continue
			}
			_, np, err := wire.DecodeFrame(b[:k], 32768)
			if !errors.Is(err, wire.ErrShortFrame) {
				t.Fatalf("prefix %d/%d: error = %v, want ErrShortFrame", k, n, err)
			}
			if np != 0 {
				t.Fatalf("prefix %d/%d: consumed %d on error", k, n, np)
			}
		}
	})
}

func FuzzDecodeFrames(f *testing.F) {
	var all []byte
	for _, v := range testVectors(f) {
		f.Add(v.raw)
		all = append(all, v.raw...)
	}
	f.Add(all)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		rest := b
		for len(rest) > 0 {
			_, n, err := wire.DecodeFrame(rest, wire.MaxPayloadSize)
			if err != nil {
				break
			}
			if n <= 0 || n > len(rest) {
				t.Fatalf("DecodeFrame consumed %d of %d bytes", n, len(rest))
			}
			rest = rest[n:]
		}
	})
}

func FuzzSettings(f *testing.F) {
	payload, err := wire.AppendSettings(nil, wire.DefaultSettings())
	if err != nil {
		f.Fatalf("AppendSettings: %v", err)
	}
	f.Add(payload)
	f.Add(make([]byte, wire.SettingsPayloadSize))

	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) != wire.SettingsPayloadSize {
			return
		}
		s, err := wire.ParseSettings(b)
		if err != nil {
			return
		}
		got, err := wire.AppendSettings(nil, s)
		if err != nil {
			t.Fatalf("AppendSettings(%+v): %v", s, err)
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("round trip = %x, want %x", got, b)
		}
	})
}

func prefixLengths(n int) []int {
	candidates := []int{0, 1, wire.HeaderSize - 1, wire.HeaderSize, wire.HeaderSize + wire.CreditSize, n - 1}
	seen := make(map[int]bool, len(candidates))
	out := candidates[:0]
	for _, k := range candidates {
		if k < 0 || k >= n || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	return out
}
