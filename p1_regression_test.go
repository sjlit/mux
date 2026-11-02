package mux

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

func newRawClientPeer(t *testing.T, cfg *Config) (*Session, net.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	cli := newSession(cfg, c1, true)
	t.Cleanup(func() {
		_ = cli.Abort()
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
	waitSettings(t, cli)
	return cli, c2
}

func TestRawDuplicateSYNClosesSession(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())
	f := wire.Frame{Type: wire.TypePSH, Flags: wire.FlagSYN, StreamID: 1}
	writeWireFrame(t, conn, f)
	waitFor(t, 2*time.Second, func() bool { return sess.NumStreams() == 1 })

	if err := tryWriteFrame(conn, f); err != nil && !sess.IsClosed() {
		t.Fatalf("duplicate SYN: %v", err)
	}
	waitFor(t, 2*time.Second, sess.IsClosed)
	if err := sess.closeError(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("close error = %v, want ErrProtocol", err)
	}
}

func TestOpenStreamFailsFastWhenPeerRefusesNewStreams(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	waitSettings(t, cli)
	waitSettings(t, srv)
	cli.peerMaxStreams.Store(0)

	start := time.Now()
	if _, err := cli.OpenStream(); !errors.Is(err, ErrRefusedStream) {
		t.Fatalf("OpenStream = %v, want ErrRefusedStream", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("OpenStream took %v, want fast failure", d)
	}
}

func TestRSTCodeMatchesStreamResetAndCause(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	if err := ss.Reset(wire.CodeProtocolError); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	_ = cs.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := cs.Read(make([]byte, 1)); !errors.Is(err, ErrStreamReset) || !errors.Is(err, ErrProtocol) {
		t.Fatalf("Read = %v, want ErrStreamReset and ErrProtocol", err)
	}
}

func TestCloseWriteHonorsWriteDeadline(t *testing.T) {
	cfg := testConfig()
	cfg.CloseTimeout = time.Second
	cli, srv, bc := newBlockingTestPair(t, cfg)

	cs, _ := openTestStream(t, cli, srv)
	bc.block.Store(true)
	writeDone := make(chan error, 1)
	go func() {
		_, err := cs.Write(make([]byte, 32<<10))
		writeDone <- err
	}()
	select {
	case <-bc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not reach the blocking connection")
	}

	if err := cs.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	start := time.Now()
	if err := cs.CloseWrite(); !errors.Is(err, ErrTimeout) {
		t.Fatalf("CloseWrite = %v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("CloseWrite took %v, want bounded by write deadline", d)
	}

	bc.unblock()
	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Write did not finish after unblocking the conn")
	}
}

func TestRawGoAwayFailsUnprocessedLocalStreams(t *testing.T) {
	cli, conn := newRawClientPeer(t, testConfig())

	type result struct {
		st  *Stream
		err error
	}
	opened := make(chan result, 1)
	go func() {
		st, err := cli.OpenStream()
		opened <- result{st: st, err: err}
	}()
	syn := readWireFrame(t, conn, 2*time.Second)
	if syn.Type != wire.TypePSH || syn.Flags&wire.FlagSYN == 0 || syn.StreamID != 1 {
		t.Fatalf("first frame = %+v, want SYN on stream 1", syn)
	}
	res := <-opened
	if res.err != nil {
		t.Fatalf("OpenStream: %v", res.err)
	}

	if err := tryWriteFrame(conn, wire.NewGOAWAYFrame(0, wire.CodeNoError)); err != nil {
		t.Fatalf("GOAWAY: %v", err)
	}
	_ = res.st.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := res.st.Read(make([]byte, 1)); !errors.Is(err, ErrRefusedStream) {
		t.Fatalf("stream Read = %v, want ErrRefusedStream", err)
	}

	code, last, ok := cli.PeerGoAway()
	if !ok || code != wire.CodeNoError || last != 0 {
		t.Fatalf("PeerGoAway = (%v, %d, %v), want (NO_ERROR, 0, true)", code, last, ok)
	}
	if _, err := cli.OpenStream(); !errors.Is(err, ErrGoAway) {
		t.Fatalf("OpenStream after GOAWAY = %v, want ErrGoAway", err)
	}
}

func TestRawGoAwayLastIDExcludesRefusedStreams(t *testing.T) {
	cfg := testConfig()
	cfg.AcceptBacklog = 1
	sess, conn := newRawPeer(t, cfg)

	writeWireFrame(t, conn, wire.Frame{Type: wire.TypePSH, Flags: wire.FlagSYN, StreamID: 1})
	waitFor(t, 2*time.Second, func() bool { return sess.NumStreams() == 1 })

	writeWireFrame(t, conn, wire.Frame{Type: wire.TypePSH, Flags: wire.FlagSYN, StreamID: 3})
	readUntil(t, conn, func(f wire.Frame) bool {
		return f.Type == wire.TypeRST && f.StreamID == 3
	})
	if got := sess.lastPeerStreamID(); got != 1 {
		t.Fatalf("lastPeerStreamID = %d, want 1 (refused stream 3 must not count)", got)
	}
}

func TestCloseGracefulRejectsUnacceptedStreams(t *testing.T) {
	cfg := testConfig()
	cfg.CloseTimeout = 2 * time.Second
	cli, srv := newTestPair(t, cfg)

	cs, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return srv.NumStreams() == 1 })

	start := time.Now()
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v, unaccepted stream should not wait for CloseTimeout", d)
	}
	_ = cs.Close()
}

func TestSessionCloseErrorAccessor(t *testing.T) {
	cli, _ := newTestPair(t, testConfig())
	if err := cli.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if err := cli.CloseError(); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("CloseError = %v, want ErrSessionClosed", err)
	}
}

func TestSessionErrorWrapsTransportFailure(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	waitSettings(t, cli)
	waitSettings(t, srv)

	if err := srv.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	waitFor(t, 2*time.Second, cli.IsClosed)

	err := cli.CloseError()
	var se *SessionError
	if !errors.As(err, &se) {
		t.Fatalf("CloseError = %T %v, want *SessionError", err, err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("CloseError = %v, want wrapped io.EOF", err)
	}
}

func TestCloseGracefulReturnsContextError(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	_, _ = openTestStream(t, cli, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := cli.CloseGraceful(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseGraceful = %v, want context deadline exceeded", err)
	}
}
