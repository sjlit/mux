package wire_test

import (
	"testing"

	"github.com/sjlit/mux/wire"
)

var benchPayload = make([]byte, 32*1024)

func BenchmarkDecodeFrame(b *testing.B) {
	raw, err := wire.AppendFrame(nil, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
		Payload:  benchPayload,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(benchPayload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := wire.DecodeFrame(raw, wire.MaxPayloadSize); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendFrame(b *testing.B) {
	f := wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
		Payload:  benchPayload,
	}
	buf := make([]byte, 0, wire.HeaderSize+len(benchPayload))

	b.SetBytes(int64(len(benchPayload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err := wire.AppendFrame(buf[:0], f)
		if err != nil {
			b.Fatal(err)
		}
		if len(dst) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkDecodeSettings(b *testing.B) {
	payload, err := wire.AppendSettings(nil, wire.DefaultSettings())
	if err != nil {
		b.Fatal(err)
	}
	raw, err := wire.AppendFrame(nil, wire.Frame{Type: wire.TypeSETTINGS, Payload: payload})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fr, _, err := wire.DecodeFrame(raw, wire.MaxPayloadSize)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := wire.ParseSettings(fr.Payload); err != nil {
			b.Fatal(err)
		}
	}
}
