package mux

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

func readWireFrame(t *testing.T, conn net.Conn, timeout time.Duration) wire.Frame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	defer conn.SetReadDeadline(time.Time{})

	var hdr [wire.HeaderSize]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	_, total, err := wire.DecodeHeader(hdr[:], wire.MaxPayloadSize)
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	buf := make([]byte, total)
	copy(buf, hdr[:])
	if _, err := io.ReadFull(conn, buf[wire.HeaderSize:]); err != nil {
		t.Fatalf("read body: %v", err)
	}
	f, n, err := wire.DecodeFrame(buf, wire.MaxPayloadSize)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if n != total {
		t.Fatalf("consumed %d, want %d", n, total)
	}
	return f
}

func writeWireFrame(t *testing.T, conn net.Conn, f wire.Frame) {
	t.Helper()
	b, err := wire.AppendFrame(nil, f)
	if err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if _, err := conn.Write(b); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readUntil(t *testing.T, conn net.Conn, match func(wire.Frame) bool) wire.Frame {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f := readWireFrame(t, conn, time.Until(deadline))
		if match(f) {
			return f
		}
	}
	t.Fatal("matching frame not received")
	return wire.Frame{}
}

// newRawPeer creates a server session and returns the raw client end. The
// session's SETTINGS is consumed and a default client SETTINGS is sent.
func newRawPeer(t *testing.T, cfg *Config) (*Session, net.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	sess := newSession(cfg, c1, false)
	t.Cleanup(func() {
		_ = sess.Abort()
		_ = c2.Close()
	})

	first := readWireFrame(t, c2, 2*time.Second)
	if first.Type != wire.TypeSETTINGS {
		t.Fatalf("first frame = %s, want SETTINGS", first.Type)
	}
	settings, err := wire.NewSettingsFrame(wire.DefaultSettings())
	if err != nil {
		t.Fatalf("NewSettingsFrame: %v", err)
	}
	writeWireFrame(t, c2, settings)
	waitSettings(t, sess)
	return sess, c2
}

func TestRawUnknownStreamGetsRST(t *testing.T) {
	cfg := testConfig()
	sess, conn := newRawPeer(t, cfg)

	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		StreamID: 1,
		Payload:  []byte("junk"),
	})
	rst := readUntil(t, conn, func(f wire.Frame) bool {
		return f.Type == wire.TypeRST && f.StreamID == 1
	})
	code, err := rst.Code()
	if err != nil {
		t.Fatalf("RST code: %v", err)
	}
	if code != wire.CodeStreamClosed {
		t.Fatalf("RST code = %s, want STREAM_CLOSED", code)
	}

	// The session must stay usable.
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 3,
		Payload:  []byte("ok"),
	})
	st, err := sess.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ok" {
		t.Fatalf("read = %q, want ok", buf)
	}
}

func TestRawDuplicateSettingsClosesSession(t *testing.T) {
	cfg := testConfig()
	sess, conn := newRawPeer(t, cfg)

	settings, err := wire.NewSettingsFrame(wire.DefaultSettings())
	if err != nil {
		t.Fatalf("NewSettingsFrame: %v", err)
	}
	writeWireFrame(t, conn, settings)

	waitFor(t, 2*time.Second, sess.IsClosed)
}

func TestRawDataAfterFINClosesSession(t *testing.T) {
	cfg := testConfig()
	sess, conn := newRawPeer(t, cfg)

	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN | wire.FlagFIN,
		StreamID: 1,
		Payload:  []byte("bye"),
	})
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		StreamID: 1,
		Payload:  []byte("late"),
	})

	waitFor(t, 2*time.Second, sess.IsClosed)
}

func TestRawFlowControlViolationClosesSession(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFrameSize = 32768
	cfg.InitialStreamWindow = 64 << 10
	cfg.InitialSessionWindow = 256 << 10
	sess, conn := newRawPeer(t, cfg)

	payload := make([]byte, 32768)
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
		Payload:  payload,
	})
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		StreamID: 1,
		Payload:  payload,
	})
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		StreamID: 1,
		Payload:  payload,
	})

	waitFor(t, 2*time.Second, sess.IsClosed)
}

func TestRawMaxConcurrentStreamsRefuses(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConcurrentStreams = 1
	sess, conn := newRawPeer(t, cfg)

	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
	})
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 3,
	})

	rst := readUntil(t, conn, func(f wire.Frame) bool {
		return f.Type == wire.TypeRST && f.StreamID == 3
	})
	code, _ := rst.Code()
	if code != wire.CodeRefusedStream {
		t.Fatalf("RST code = %s, want REFUSED_STREAM", code)
	}

	st, err := sess.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if st.ID() != 1 {
		t.Fatalf("accepted stream %d, want 1", st.ID())
	}
}

func TestRawPeerStreamIDParity(t *testing.T) {
	cfg := testConfig()
	sess, conn := newRawPeer(t, cfg)

	// A server expects odd peer-initiated IDs; even is a protocol error.
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 2,
	})
	waitFor(t, 2*time.Second, sess.IsClosed)
}

func TestRawPeerStreamIDMustIncrease(t *testing.T) {
	cfg := testConfig()
	sess, conn := newRawPeer(t, cfg)

	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 3,
	})
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
	})
	waitFor(t, 2*time.Second, sess.IsClosed)
}
