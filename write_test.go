package mux

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

func newTestPairWrapped(t *testing.T, cfg *Config, wrap func(net.Conn) net.Conn) (*Session, *Session) {
	t.Helper()
	c1, c2 := net.Pipe()
	if wrap != nil {
		c1 = wrap(c1)
	}
	cli := newSession(cfg, c1, true)
	srv := newSession(cfg, c2, false)
	t.Cleanup(func() {
		_ = cli.Abort()
		_ = srv.Abort()
	})
	return cli, srv
}

type writeBuffersConn struct {
	net.Conn
	calls atomic.Int32
}

func (c *writeBuffersConn) WriteBuffers(v [][]byte) (int, error) {
	c.calls.Add(1)
	total := 0
	for _, b := range v {
		n, err := c.Conn.Write(b)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestWriteBuffersPath(t *testing.T) {
	cfg := testConfig()
	wb := &writeBuffersConn{}
	cli, srv := newTestPairWrapped(t, cfg, func(c net.Conn) net.Conn {
		wb.Conn = c
		return wb
	})
	waitSettings(t, cli)
	waitSettings(t, srv)
	cs, ss := openTestStream(t, cli, srv)

	payload := bytes.Repeat([]byte{0x5a}, 100*1024)
	writeErr := make(chan error, 1)
	go func() {
		_, err := cs.Write(payload)
		writeErr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(ss, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if wb.calls.Load() == 0 {
		t.Fatal("WriteBuffers was not used")
	}
	_ = cs.Close()
	_ = ss.Close()
}

type faultConn struct {
	net.Conn
	short atomic.Bool
	fail  atomic.Bool
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.fail.Load() {
		return 0, errors.New("injected write failure")
	}
	if c.short.Load() {
		n := len(p) / 2
		if n == 0 {
			n = 1
		}
		return c.Conn.Write(p[:n])
	}
	return c.Conn.Write(p)
}

func TestShortWriteFailsSession(t *testing.T) {
	cfg := testConfig()
	fc := &faultConn{}
	cli, srv := newTestPairWrapped(t, cfg, func(c net.Conn) net.Conn {
		fc.Conn = c
		return fc
	})
	waitSettings(t, cli)
	waitSettings(t, srv)
	cs, _ := openTestStream(t, cli, srv)

	fc.short.Store(true)
	_, err := cs.Write([]byte("hello world"))
	if err == nil {
		t.Fatal("Write succeeded despite short write")
	}
	waitFor(t, 2*time.Second, cli.IsClosed)
}

func TestWriteErrorFailsSession(t *testing.T) {
	cfg := testConfig()
	fc := &faultConn{}
	cli, srv := newTestPairWrapped(t, cfg, func(c net.Conn) net.Conn {
		fc.Conn = c
		return fc
	})
	waitSettings(t, cli)
	waitSettings(t, srv)
	cs, _ := openTestStream(t, cli, srv)

	fc.fail.Store(true)
	_, err := cs.Write([]byte("hello world"))
	if err == nil {
		t.Fatal("Write succeeded despite injected error")
	}
	waitFor(t, 2*time.Second, cli.IsClosed)
}

// fixedConn returns a caller-controlled result from WriteBuffers. It is used
// to exercise writeFrames' partial-write accounting without a real transport.
type fixedConn struct {
	n      int
	err    error
	closed atomic.Bool
}

func (c *fixedConn) Read([]byte) (int, error)  { return 0, io.EOF }
func (c *fixedConn) Write([]byte) (int, error) { return 0, errors.New("unexpected Write") }
func (c *fixedConn) Close() error {
	c.closed.Store(true)
	return nil
}
func (c *fixedConn) WriteBuffers([][]byte) (int, error) { return c.n, c.err }

func newWriteFramesSession(conn io.ReadWriteCloser) *Session {
	return &Session{
		conn:         conn,
		die:          make(chan struct{}),
		streams:      newStreamTable(),
		chAccepts:    make(chan *Stream),
		stateNotify:  newNotifier(),
		creditNotify: newNotifier(),
		wake:         make(chan struct{}, 1),
	}
}

func writeFramesForTest(t *testing.T, s *Session, ctrl []ctrlMsg, data []*writeBatch) bool {
	t.Helper()
	var hdrArena []byte
	var vec [][]byte
	var infos []frameInfo
	return s.writeFrames(ctrl, data, &hdrArena, &vec, &infos)
}

func TestWriteFramesPartialControlPrefix(t *testing.T) {
	conn := &fixedConn{n: wire.HeaderSize + wire.CreditSize}
	s := newWriteFramesSession(conn)
	st := &Stream{id: 1, sess: s}
	b := &writeBatch{stream: st, data: []byte("hello")}
	ctrl := []ctrlMsg{{frame: wire.NewUPDFrame(1, 0)}}

	if writeFramesForTest(t, s, ctrl, []*writeBatch{b}) {
		t.Fatal("writeFrames succeeded after a control-prefix-only write")
	}
	if b.n != 0 {
		t.Fatalf("data batch wrote %d bytes, want 0", b.n)
	}
	if b.err == nil {
		t.Fatal("data batch has no error")
	}
	if !conn.closed.Load() {
		t.Fatal("session did not close after short write")
	}
}

func TestWriteFramesFullWriteWithError(t *testing.T) {
	boom := errors.New("boom")
	conn := &fixedConn{n: wire.HeaderSize + len("hello"), err: boom}
	s := newWriteFramesSession(conn)
	st := &Stream{id: 1, sess: s}
	b := &writeBatch{stream: st, data: []byte("hello")}

	if writeFramesForTest(t, s, nil, []*writeBatch{b}) {
		t.Fatal("writeFrames succeeded despite write error")
	}
	if b.n != len("hello") {
		t.Fatalf("batch wrote %d bytes, want %d", b.n, len("hello"))
	}
	if !errors.Is(b.err, boom) {
		t.Fatalf("batch error = %v, want %v", b.err, boom)
	}
}

func TestWriteFramesBuildFailureCompletesAllData(t *testing.T) {
	conn := &fixedConn{}
	s := newWriteFramesSession(conn)
	good := &writeBatch{stream: &Stream{id: 1, sess: s}, data: []byte("a")}
	bad := &writeBatch{stream: &Stream{id: 0, sess: s}, data: []byte("b")}

	if writeFramesForTest(t, s, nil, []*writeBatch{good, bad}) {
		t.Fatal("writeFrames succeeded with an invalid stream id")
	}
	for i, b := range []*writeBatch{good, bad} {
		if !b.completed.Load() {
			t.Fatalf("batch %d was not completed", i)
		}
		if b.n != 0 {
			t.Fatalf("batch %d wrote %d bytes, want 0", i, b.n)
		}
	}
}

func TestResetWaitsForInFlightWrite(t *testing.T) {
	s := newWriteFramesSession(&fixedConn{})
	st := newStream(s, 1, true, 0)

	st.writeMu.Lock()
	done := make(chan struct{})
	go func() {
		_ = st.Reset(wire.CodeCancel)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Reset returned while a write held writeMu")
	case <-time.After(20 * time.Millisecond):
	}

	st.writeMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Reset did not return after writeMu was released")
	}

	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()
	if got := len(s.ctrlQ) - s.ctrlHead; got != 1 || s.ctrlQ[s.ctrlHead].frame.Type != wire.TypeRST {
		t.Fatalf("control queue = %#v (head %d), want one RST", s.ctrlQ, s.ctrlHead)
	}
}
