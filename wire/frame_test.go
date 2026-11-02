package wire_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/sjlit/mux/wire"
)

type vector struct {
	name  string
	frame wire.Frame
	raw   []byte
}

func testVectors(tb testing.TB) []vector {
	tb.Helper()
	settingsPayload := mustHex(tb, "03 00 00 00 00 80 00 00 00 00 10 00 00 00 80 00 10 27 00 00 30 75 00 00 FF FF FF FF")
	return []vector{
		{
			name:  "psh-syn",
			frame: wire.Frame{Type: wire.TypePSH, Flags: wire.FlagSYN, StreamID: 1, Payload: []byte("hello")},
			raw:   mustHex(tb, "03 01 05 00 01 00 00 00 68 65 6C 6C 6F"),
		},
		{
			name: "psh-fin-credit",
			frame: wire.Frame{
				Type:     wire.TypePSH,
				Flags:    wire.FlagFIN | wire.FlagCredit,
				StreamID: 1,
				Credit:   65536,
				Payload:  []byte("hi"),
			},
			raw: mustHex(tb, "03 06 02 00 01 00 00 00 00 00 01 00 68 69"),
		},
		{
			name:  "upd-session",
			frame: wire.NewUPDFrame(0, 1<<20),
			raw:   mustHex(tb, "03 10 04 00 00 00 00 00 00 00 10 00"),
		},
		{
			name:  "rst",
			frame: wire.NewRSTFrame(3, wire.CodeCancel),
			raw:   mustHex(tb, "03 20 04 00 03 00 00 00 07 00 00 00"),
		},
		{
			name:  "ping",
			frame: wire.NewPINGFrame(42),
			raw:   mustHex(tb, "03 30 00 00 2A 00 00 00"),
		},
		{
			name:  "pong",
			frame: wire.NewPONGFrame(42),
			raw:   mustHex(tb, "03 40 00 00 2A 00 00 00"),
		},
		{
			name:  "goaway",
			frame: wire.NewGOAWAYFrame(99, wire.CodeNoError),
			raw:   mustHex(tb, "03 50 04 00 63 00 00 00 00 00 00 00"),
		},
		{
			name:  "settings",
			frame: wire.Frame{Type: wire.TypeSETTINGS, Payload: settingsPayload},
			raw:   mustHex(tb, "03 60 1C 00 00 00 00 00 03 00 00 00 00 80 00 00 00 00 10 00 00 00 80 00 10 27 00 00 30 75 00 00 FF FF FF FF"),
		},
	}
}

func TestVectors(t *testing.T) {
	for _, tc := range testVectors(t) {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := wire.AppendFrame(nil, tc.frame)
			if err != nil {
				t.Fatalf("AppendFrame: %v", err)
			}
			if !bytes.Equal(raw, tc.raw) {
				t.Fatalf("AppendFrame = %x, want %x", raw, tc.raw)
			}

			got, n, err := wire.DecodeFrame(tc.raw, wire.MaxPayloadSize)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}
			if n != len(tc.raw) {
				t.Fatalf("DecodeFrame consumed %d, want %d", n, len(tc.raw))
			}
			if !framesEqual(got, tc.frame) {
				t.Fatalf("DecodeFrame = %+v, want %+v", got, tc.frame)
			}

			n, err = wire.EncodedLen(tc.frame)
			if err != nil {
				t.Fatalf("EncodedLen: %v", err)
			}
			if n != len(tc.raw) {
				t.Fatalf("EncodedLen = %d, want %d", n, len(tc.raw))
			}
		})
	}
}

func TestDecodeHeader(t *testing.T) {
	for _, tc := range testVectors(t) {
		t.Run(tc.name, func(t *testing.T) {
			fr, total, err := wire.DecodeHeader(tc.raw[:wire.HeaderSize], wire.MaxPayloadSize)
			if err != nil {
				t.Fatalf("DecodeHeader: %v", err)
			}
			if total != len(tc.raw) {
				t.Fatalf("total = %d, want %d", total, len(tc.raw))
			}
			if fr.Type != tc.frame.Type || fr.Flags != tc.frame.Flags || fr.StreamID != tc.frame.StreamID {
				t.Fatalf("header = %+v, want type=%v flags=%v sid=%d", fr, tc.frame.Type, tc.frame.Flags, tc.frame.StreamID)
			}
			if len(fr.Payload) != 0 || fr.Credit != 0 {
				t.Fatalf("DecodeHeader returned payload %d credit %d, want 0/0", len(fr.Payload), fr.Credit)
			}
		})
	}

	if _, _, err := wire.DecodeHeader(mustHex(t, "03 00 00"), wire.MaxPayloadSize); !errors.Is(err, wire.ErrShortFrame) {
		t.Fatalf("short header error = %v, want ErrShortFrame", err)
	}
	if _, _, err := wire.DecodeHeader(mustHex(t, "02 00 00 00 01 00 00 00"), wire.MaxPayloadSize); !errors.Is(err, wire.ErrBadVersion) {
		t.Fatalf("bad version error = %v, want ErrBadVersion", err)
	}
}

func TestAppendHeader(t *testing.T) {
	for _, tc := range testVectors(t) {
		t.Run(tc.name, func(t *testing.T) {
			wantLen := wire.HeaderSize
			if tc.frame.Flags&wire.FlagCredit != 0 {
				wantLen += wire.CreditSize
			}
			got, err := wire.AppendHeader(nil, tc.frame)
			if err != nil {
				t.Fatalf("AppendHeader: %v", err)
			}
			if !bytes.Equal(got, tc.raw[:wantLen]) {
				t.Fatalf("AppendHeader = %x, want %x", got, tc.raw[:wantLen])
			}
		})
	}
}

func TestDecodeConcatenatedFrames(t *testing.T) {
	vecs := testVectors(t)
	var buf []byte
	for _, v := range vecs {
		buf = append(buf, v.raw...)
	}

	rest := buf
	for i, v := range vecs {
		got, n, err := wire.DecodeFrame(rest, wire.MaxPayloadSize)
		if err != nil {
			t.Fatalf("frame %d (%s): DecodeFrame: %v", i, v.name, err)
		}
		if !framesEqual(got, v.frame) {
			t.Fatalf("frame %d (%s): got %+v, want %+v", i, v.name, got, v.frame)
		}
		rest = rest[n:]
	}
	if len(rest) != 0 {
		t.Fatalf("%d trailing bytes", len(rest))
	}
}

func TestDecodeIgnoresTrailingBytes(t *testing.T) {
	raw := mustHex(t, "03 00 05 00 01 00 00 00 68 65 6C 6C 6F")
	withTrailing := append(append([]byte(nil), raw...), 0xde, 0xad)

	got, n, err := wire.DecodeFrame(withTrailing, wire.MaxPayloadSize)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if n != len(raw) {
		t.Fatalf("consumed %d, want %d", n, len(raw))
	}
	if string(got.Payload) != "hello" {
		t.Fatalf("payload = %q, want hello", got.Payload)
	}
}

func TestDecodePayloadAliasesInput(t *testing.T) {
	raw := mustHex(t, "03 00 05 00 01 00 00 00 68 65 6C 6C 6F")
	fr, _, err := wire.DecodeFrame(raw, wire.MaxPayloadSize)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}

	raw[wire.HeaderSize] = 'H'
	if fr.Payload[0] != 'H' {
		t.Fatal("DecodeFrame copied the payload instead of aliasing the input")
	}
}

func TestDecodeFrameSizeLimit(t *testing.T) {
	raw := rawPSH(1000, 1)

	if _, n, err := wire.DecodeFrame(raw, 0); err != nil || n != len(raw) {
		t.Fatalf("maxFrameSize=0: n=%d err=%v", n, err)
	}
	if _, _, err := wire.DecodeFrame(raw, 999); !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("maxFrameSize=999: err=%v, want ErrFrameTooLarge", err)
	}
	if _, n, err := wire.DecodeFrame(raw, 1000); err != nil || n != len(raw) {
		t.Fatalf("maxFrameSize=1000: n=%d err=%v", n, err)
	}
}

func TestDecodeInvalidFrames(t *testing.T) {
	tooLarge := rawPSH(40000, 1)

	tests := []struct {
		name string
		raw  []byte
		max  uint32
		want error
	}{
		{"empty", nil, wire.MaxPayloadSize, wire.ErrShortFrame},
		{"short-header", mustHex(t, "03 00 00"), wire.MaxPayloadSize, wire.ErrShortFrame},
		{"bad-version", mustHex(t, "02 00 00 00 01 00 00 00"), wire.MaxPayloadSize, wire.ErrBadVersion},
		{"bad-type", mustHex(t, "03 70 00 00 01 00 00 00"), wire.MaxPayloadSize, wire.ErrBadType},
		{"reserved-flag", mustHex(t, "03 08 00 00 01 00 00 00"), wire.MaxPayloadSize, wire.ErrBadFlags},
		{"syn-on-upd", mustHex(t, "03 11 04 00 00 00 00 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadFlags},
		{"upd-bad-len", mustHex(t, "03 10 03 00 00 00 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadLength},
		{"rst-bad-len", mustHex(t, "03 20 05 00 03 00 00 00 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadLength},
		{"ping-bad-len", mustHex(t, "03 30 01 00 2A 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadLength},
		{"goaway-bad-len", mustHex(t, "03 50 03 00 63 00 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadLength},
		{"settings-bad-len", mustHex(t, "03 60 1B 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadLength},
		{"settings-bad-sid", mustHex(t, "03 60 1C 00 01 00 00 00"), wire.MaxPayloadSize, wire.ErrBadStreamID},
		{"psh-sid-zero", mustHex(t, "03 00 00 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrBadStreamID},
		{"rst-sid-zero", mustHex(t, "03 20 04 00 00 00 00 00 07 00 00 00"), wire.MaxPayloadSize, wire.ErrBadStreamID},
		{"truncated-payload", mustHex(t, "03 00 05 00 01 00 00 00 68 69"), wire.MaxPayloadSize, wire.ErrShortFrame},
		{"truncated-credit", mustHex(t, "03 04 00 00 01 00 00 00 00 00 00"), wire.MaxPayloadSize, wire.ErrShortFrame},
		{"frame-too-large", tooLarge, 32768, wire.ErrFrameTooLarge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, n, err := wire.DecodeFrame(tc.raw, tc.max)
			if !errors.Is(err, tc.want) {
				t.Fatalf("DecodeFrame error = %v, want %v", err, tc.want)
			}
			if n != 0 {
				t.Fatalf("DecodeFrame consumed %d on error, want 0", n)
			}
		})
	}
}

func TestAppendInvalidFrames(t *testing.T) {
	tests := []struct {
		name  string
		frame wire.Frame
		want  error
	}{
		{"bad-type", wire.Frame{Type: wire.Type(7), StreamID: 1}, wire.ErrBadType},
		{"reserved-flags", wire.Frame{Type: wire.TypePSH, Flags: 0x8, StreamID: 1}, wire.ErrBadFlags},
		{"flags-on-upd", wire.Frame{Type: wire.TypeUPD, Flags: wire.FlagSYN, Payload: make([]byte, 4)}, wire.ErrBadFlags},
		{"credit-without-flag", wire.Frame{Type: wire.TypePSH, StreamID: 1, Credit: 1}, wire.ErrBadFlags},
		{"upd-bad-len", wire.Frame{Type: wire.TypeUPD, Payload: make([]byte, 3)}, wire.ErrBadLength},
		{"rst-bad-len", wire.Frame{Type: wire.TypeRST, StreamID: 3, Payload: make([]byte, 3)}, wire.ErrBadLength},
		{"ping-bad-len", wire.Frame{Type: wire.TypePING, StreamID: 1, Payload: []byte{0}}, wire.ErrBadLength},
		{"goaway-bad-len", wire.Frame{Type: wire.TypeGOAWAY, StreamID: 1, Payload: make([]byte, 3)}, wire.ErrBadLength},
		{"settings-bad-len", wire.Frame{Type: wire.TypeSETTINGS, Payload: make([]byte, 27)}, wire.ErrBadLength},
		{"settings-bad-sid", wire.Frame{Type: wire.TypeSETTINGS, StreamID: 1, Payload: make([]byte, wire.SettingsPayloadSize)}, wire.ErrBadStreamID},
		{"psh-sid-zero", wire.Frame{Type: wire.TypePSH}, wire.ErrBadStreamID},
		{"rst-sid-zero", wire.Frame{Type: wire.TypeRST, Payload: make([]byte, 4)}, wire.ErrBadStreamID},
		{"psh-too-large", wire.Frame{Type: wire.TypePSH, StreamID: 1, Payload: make([]byte, wire.MaxPayloadSize+1)}, wire.ErrFrameTooLarge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := wire.AppendFrame(nil, tc.frame); !errors.Is(err, tc.want) {
				t.Fatalf("AppendFrame error = %v, want %v", err, tc.want)
			}
			if _, err := wire.EncodedLen(tc.frame); !errors.Is(err, tc.want) {
				t.Fatalf("EncodedLen error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestFramePayloadHelpers(t *testing.T) {
	upd := wire.NewUPDFrame(7, 123)
	if got, err := upd.Consumed(); err != nil || got != 123 {
		t.Fatalf("Consumed = %d, %v; want 123, nil", got, err)
	}
	if _, err := upd.Code(); !errors.Is(err, wire.ErrBadType) {
		t.Fatalf("UPD.Code error = %v, want ErrBadType", err)
	}

	rst := wire.NewRSTFrame(3, wire.CodeCancel)
	if got, err := rst.Code(); err != nil || got != wire.CodeCancel {
		t.Fatalf("Code = %v, %v; want %v, nil", got, err, wire.CodeCancel)
	}
	if _, err := rst.Consumed(); !errors.Is(err, wire.ErrBadType) {
		t.Fatalf("RST.Consumed error = %v, want ErrBadType", err)
	}

	goaway := wire.NewGOAWAYFrame(99, wire.CodeNoError)
	if got, err := goaway.Code(); err != nil || got != wire.CodeNoError {
		t.Fatalf("Code = %v, %v; want %v, nil", got, err, wire.CodeNoError)
	}

	bad := wire.Frame{Type: wire.TypeUPD, Payload: make([]byte, 3)}
	if _, err := bad.Consumed(); !errors.Is(err, wire.ErrBadLength) {
		t.Fatalf("short UPD.Consumed error = %v, want ErrBadLength", err)
	}

	if _, err := (&wire.Frame{Type: wire.TypeRST, StreamID: 3, Payload: make([]byte, 3)}).Code(); !errors.Is(err, wire.ErrBadLength) {
		t.Fatalf("short RST.Code error = %v, want ErrBadLength", err)
	}
	if _, err := (&wire.Frame{Type: wire.TypeGOAWAY, Payload: make([]byte, 5)}).Code(); !errors.Is(err, wire.ErrBadLength) {
		t.Fatalf("short GOAWAY.Code error = %v, want ErrBadLength", err)
	}
}

func TestStringers(t *testing.T) {
	types := map[wire.Type]string{
		wire.TypePSH:      "PSH",
		wire.TypeUPD:      "UPD",
		wire.TypeRST:      "RST",
		wire.TypePING:     "PING",
		wire.TypePONG:     "PONG",
		wire.TypeGOAWAY:   "GOAWAY",
		wire.TypeSETTINGS: "SETTINGS",
		wire.Type(7):      "Type(0x7)",
		wire.Type(0xff):   "Type(0xff)",
	}
	for typ, want := range types {
		if got := typ.String(); got != want {
			t.Fatalf("%d.String() = %q, want %q", uint8(typ), got, want)
		}
	}

	codes := map[wire.ErrorCode]string{
		wire.CodeNoError:           "NO_ERROR",
		wire.CodeProtocolError:     "PROTOCOL_ERROR",
		wire.CodeFlowControlError:  "FLOW_CONTROL_ERROR",
		wire.CodeStreamClosed:      "STREAM_CLOSED",
		wire.CodeStreamIDExhausted: "STREAM_ID_EXHAUSTED",
		wire.CodeFrameSizeError:    "FRAME_SIZE_ERROR",
		wire.CodeInternalError:     "INTERNAL_ERROR",
		wire.CodeCancel:            "CANCEL",
		wire.CodeRefusedStream:     "REFUSED_STREAM",
		wire.CodeEnhanceYourCalm:   "ENHANCE_YOUR_CALM",
		wire.ErrorCode(0x0a):       "ErrorCode(0x0a)",
	}
	for code, want := range codes {
		if got := code.String(); got != want {
			t.Fatalf("%d.String() = %q, want %q", uint32(code), got, want)
		}
	}
}

func TestNewSettingsFrameMatchesVector(t *testing.T) {
	f, err := wire.NewSettingsFrame(wire.DefaultSettings())
	if err != nil {
		t.Fatalf("NewSettingsFrame: %v", err)
	}
	raw, err := wire.AppendFrame(nil, f)
	if err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	want := mustHex(t, "03 60 1C 00 00 00 00 00 03 00 00 00 00 80 00 00 00 00 10 00 00 00 80 00 10 27 00 00 30 75 00 00 FF FF FF FF")
	if !bytes.Equal(raw, want) {
		t.Fatalf("NewSettingsFrame = %x, want %x", raw, want)
	}
}

func framesEqual(a, b wire.Frame) bool {
	return a.Type == b.Type &&
		a.Flags == b.Flags &&
		a.StreamID == b.StreamID &&
		a.Credit == b.Credit &&
		bytes.Equal(a.Payload, b.Payload)
}

func mustHex(tb testing.TB, s string) []byte {
	tb.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		tb.Fatalf("invalid hex %q: %v", s, err)
	}
	return b
}

func rawPSH(length int, sid uint32) []byte {
	b := make([]byte, wire.HeaderSize+length)
	b[0] = wire.Version
	b[1] = byte(wire.TypePSH) << 4
	binary.LittleEndian.PutUint16(b[2:4], uint16(length))
	binary.LittleEndian.PutUint32(b[4:8], sid)
	return b
}
