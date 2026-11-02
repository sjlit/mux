package mux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

// TestSessionCreditFlushedBeforeReadBlocks exercises the cross-stream session
// window deadlock: the application drains one stream, then waits for more
// data on it while other streams hold the rest of the session window.
func TestSessionCreditFlushedBeforeReadBlocks(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFrameSize = 32 << 10
	cfg.InitialStreamWindow = 64 << 10
	cfg.InitialSessionWindow = 256 << 10
	cfg.WriteBatchSize = 64 << 10

	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	const streams = 4
	pairs := make([]struct{ cli, srv *Stream }, streams)
	payload := bytes.Repeat([]byte{0x5a}, 64<<10)
	for i := range pairs {
		pairs[i].cli, pairs[i].srv = openTestStream(t, cli, srv)
		if _, err := pairs[i].cli.Write(payload); err != nil {
			t.Fatalf("stream %d write: %v", i, err)
		}
	}

	// A second write on the first stream is blocked by the stream window.
	// Once the reader consumes the first write it still has stream credit, but
	// the session window is exhausted by the other three streams.
	writeDone := make(chan error, 1)
	go func() {
		_, err := pairs[0].cli.Write(payload)
		writeDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-writeDone:
		t.Fatalf("second write completed before credit was freed: %v", err)
	default:
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(pairs[0].srv, got); err != nil {
		t.Fatalf("first read: %v", err)
	}
	select {
	case err := <-writeDone:
		t.Fatalf("second write completed without a session credit update: %v", err)
	default:
	}

	_ = pairs[0].srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(pairs[0].srv, got); err != nil {
		t.Fatalf("read after credit flush: %v", err)
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("second write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second write did not complete")
	}
}

// TestSessionCreditPressureReleasesBlockedReader covers the case where the
// application is already blocked before the rest of the session window fills:
// data arrival must publish the consumed credit.
func TestSessionCreditPressureReleasesBlockedReader(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFrameSize = 32 << 10
	cfg.InitialStreamWindow = 64 << 10
	cfg.InitialSessionWindow = 256 << 10
	cfg.WriteBatchSize = 64 << 10

	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	const streams = 4
	pairs := make([]struct{ cli, srv *Stream }, streams)
	for i := range pairs {
		pairs[i].cli, pairs[i].srv = openTestStream(t, cli, srv)
	}
	payload := bytes.Repeat([]byte{0x6b}, 64<<10)
	if _, err := pairs[0].cli.Write(payload); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, err := io.ReadFull(pairs[0].srv, make([]byte, len(payload))); err != nil {
		t.Fatalf("first read: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(pairs[0].srv, make([]byte, len(payload)))
		readDone <- err
	}()
	time.Sleep(50 * time.Millisecond)

	// These three streams fill the session window while the reader is already
	// blocked on stream 0.
	for i := 1; i < streams; i++ {
		if _, err := pairs[i].cli.Write(payload); err != nil {
			t.Fatalf("stream %d write: %v", i, err)
		}
	}
	waitFor(t, 2*time.Second, func() bool {
		return cli.peerSessionConsumed.Load() >= uint32(len(payload))
	})

	writeDone := make(chan error, 1)
	go func() {
		_, err := pairs[0].cli.Write(payload)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write after pressure update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write stayed blocked after session pressure update")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("blocked read: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked read was not released")
	}
}

// TestUnknownStreamBudgetRefills checks that the discard budget is a token
// bucket rather than a permanent lifetime allowance.
func TestUnknownStreamBudgetRefills(t *testing.T) {
	const max = int64(1 << 20)
	s := &Session{unknownBudgetMax: max}
	now := time.Now()
	s.unknownBudget = 0
	s.unknownBudgetUpdated = now

	s.refillUnknownBudget(now.Add(unknownBudgetRefillPeriod / 2))
	if s.unknownBudget < max/2-max/100 || s.unknownBudget > max/2+max/100 {
		t.Fatalf("half-period refill = %d, want about %d", s.unknownBudget, max/2)
	}

	s.unknownBudget = 0
	s.unknownBudgetUpdated = now
	s.refillUnknownBudget(now.Add(unknownBudgetRefillPeriod))
	if s.unknownBudget != max {
		t.Fatalf("full-period refill = %d, want %d", s.unknownBudget, max)
	}

	s.unknownBudget = 0
	s.unknownBudgetUpdated = now
	s.refillUnknownBudget(now.Add(unknownBudgetRefillPeriod * 2))
	if s.unknownBudget != max {
		t.Fatalf("refill did not clamp to max: %d", s.unknownBudget)
	}
}

// TestCloseSendsRSTForUnreadData verifies that Close tells the peer to stop
// when local unread data is being discarded.
func TestCloseSendsRSTForUnreadData(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	if _, err := ss.Write([]byte("unread")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return !cs.ringEmpty()
	})

	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_ = ss.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := ss.Read(make([]byte, 1)); !errors.Is(err, ErrStreamReset) {
		t.Fatalf("peer Read = %v, want ErrStreamReset", err)
	}

	// A reset stream must not damage the session.
	cs2, ss2 := openTestStream(t, cli, srv)
	if _, err := cs2.Write([]byte("ok")); err != nil {
		t.Fatalf("second stream write: %v", err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(ss2, buf); err != nil || string(buf) != "ok" {
		t.Fatalf("second stream read = %q, %v", buf, err)
	}
	_ = cs2.Close()
	_ = ss2.Close()
}

// blockingWriteConn blocks Write calls after block is set, letting a test hold
// the session's connMu and writeMu while another goroutine calls Close.
type blockingWriteConn struct {
	net.Conn
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
	relOnce sync.Once
}

func newBlockingWriteConn(c net.Conn) *blockingWriteConn {
	return &blockingWriteConn{
		Conn:    c,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *blockingWriteConn) Write(p []byte) (int, error) {
	if c.block.Load() {
		c.once.Do(func() { close(c.started) })
		<-c.release
	}
	return c.Conn.Write(p)
}

func (c *blockingWriteConn) unblock() {
	c.relOnce.Do(func() { close(c.release) })
}

func newBlockingTestPair(t *testing.T, cfg *Config) (*Session, *Session, *blockingWriteConn) {
	t.Helper()
	c1, c2 := net.Pipe()
	bc := newBlockingWriteConn(c1)
	cli := newSession(cfg, bc, true)
	srv := newSession(cfg, c2, false)
	t.Cleanup(func() {
		bc.unblock()
		_ = cli.Abort()
		_ = srv.Abort()
	})
	waitSettings(t, cli)
	waitSettings(t, srv)
	return cli, srv, bc
}

func TestCloseDoesNotWaitForBlockedWrite(t *testing.T) {
	cfg := testConfig()
	cfg.CloseTimeout = 100 * time.Millisecond
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

	start := time.Now()
	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.Close() }()
	select {
	case err := <-closeDone:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("Close = %v, want ErrTimeout", err)
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Close took %v, want bounded by CloseTimeout", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked behind a blocked Write")
	}

	// A SYN queued behind the blocked writer must not make OpenStreamContext
	// wait past its context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	openStart := time.Now()
	if _, err := cli.OpenStreamContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStreamContext = %v, want context deadline exceeded", err)
	}
	if d := time.Since(openStart); d > time.Second {
		t.Fatalf("OpenStreamContext took %v, want bounded by context", d)
	}

	bc.unblock()
	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Write did not finish after unblocking the conn")
	}
}

func TestResetDoesNotWaitForBlockedWrite(t *testing.T) {
	cfg := testConfig()
	cfg.CloseTimeout = 100 * time.Millisecond
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

	start := time.Now()
	if err := cs.Reset(wire.CodeCancel); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Reset took %v behind a blocked Write", d)
	}

	bc.unblock()
	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Write did not finish after unblocking the conn")
	}
}
