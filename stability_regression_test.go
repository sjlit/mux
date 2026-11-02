package mux

import (
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

type eofAfterDataConn struct {
	net.Conn
	inject atomic.Bool
	done   atomic.Bool
	last   atomic.Value
}

func (c *eofAfterDataConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.inject.Load() && c.done.CompareAndSwap(false, true) {
		// net.Pipe may return only part of a frame. Complete the frame so the
		// test exercises a reader that returns a whole frame plus io.EOF.
		if n >= wire.HeaderSize {
			if _, total, derr := wire.DecodeHeader(b[:n], wire.MaxPayloadSize); derr == nil && total > n && total <= len(b) {
				for n < total {
					r, rerr := c.Conn.Read(b[n:total])
					n += r
					if rerr != nil {
						err = rerr
						break
					}
				}
			}
		}
		c.last.Store(append([]byte(nil), b[:n]...))
		return n, io.EOF
	}
	return n, err
}

func tryWriteFrame(conn net.Conn, f wire.Frame) error {
	b, err := wire.AppendFrame(nil, f)
	if err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

func TestBatchedKeepAliveWithTraffic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBatchSize = 8 << 10
	cfg.KeepAliveInterval = 20 * time.Millisecond
	cfg.KeepAliveTimeout = 100 * time.Millisecond

	cli, srv := newTestPair(t, cfg)
	cs, ss := openTestStream(t, cli, srv)

	stop := make(chan struct{})
	defer close(stop)

	go func() {
		buf := make([]byte, 1)
		for {
			if _, err := ss.Read(buf); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := cs.Write([]byte{'x'}); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(350 * time.Millisecond)
	if cli.IsClosed() {
		t.Fatalf("client session closed during active batched traffic: %v", cli.closeError())
	}
	if srv.IsClosed() {
		t.Fatalf("server session closed during active batched traffic: %v", srv.closeError())
	}
}

func TestBatchedFirstFrameMustBeSettings(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBatchSize = 8 << 10
	cfg.KeepAliveDisabled = true

	c1, c2 := net.Pipe()
	sess := newSession(cfg, c1, false)
	defer sess.Abort()
	defer c2.Close()

	_ = readWireFrame(t, c2, time.Second) // server SETTINGS
	writeWireFrame(t, c2, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
		Payload:  []byte("early"),
	})

	waitFor(t, 2*time.Second, sess.IsClosed)
}

func TestOpenStreamSlotTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.OpenTimeout = 60 * time.Millisecond
	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	cli.peerMaxStreams.Store(1)
	first, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("first OpenStream: %v", err)
	}
	defer first.Close()

	start := time.Now()
	if _, err := cli.OpenStream(); !errors.Is(err, ErrTimeout) {
		t.Fatalf("second OpenStream = %v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("OpenStream timed out after %v, want near %v", d, cfg.OpenTimeout)
	}
}

func TestOpenStreamSlotWakeup(t *testing.T) {
	cfg := testConfig()
	cfg.OpenTimeout = 2 * time.Second
	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	cli.peerMaxStreams.Store(1)
	first, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("first OpenStream: %v", err)
	}

	type result struct {
		st  *Stream
		err error
	}
	opened := make(chan result, 1)
	go func() {
		st, err := cli.OpenStream()
		opened <- result{st: st, err: err}
	}()

	time.Sleep(20 * time.Millisecond)
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	select {
	case res := <-opened:
		if res.err != nil {
			t.Fatalf("waiting OpenStream: %v", res.err)
		}
		_ = res.st.Close()
	case <-time.After(time.Second):
		t.Fatal("waiting OpenStream was not woken by stream close")
	}
}

func TestOpenStreamConcurrentLimit(t *testing.T) {
	cfg := testConfig()
	cfg.OpenTimeout = 80 * time.Millisecond
	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	cli.peerMaxStreams.Store(1)

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	results := make(chan error, workers)
	streams := make(chan *Stream, workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			st, err := cli.OpenStream()
			if err == nil {
				streams <- st
			}
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	close(streams)

	success := 0
	timeouts := 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrTimeout):
			timeouts++
		default:
			t.Fatalf("OpenStream: %v", err)
		}
	}
	for st := range streams {
		_ = st.Close()
	}
	if success != 1 {
		t.Fatalf("successful opens = %d, want 1", success)
	}
	if timeouts != workers-1 {
		t.Fatalf("timed-out opens = %d, want %d", timeouts, workers-1)
	}
}

func TestNilConnectionIsRejected(t *testing.T) {
	if _, err := Client(nil, nil); err == nil {
		t.Fatal("Client(nil) succeeded")
	}
	if _, err := Server(nil, nil); err == nil {
		t.Fatal("Server(nil) succeeded")
	}
}

func TestZeroLengthUnknownPSHIsIgnored(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())
	for i := 0; i < 2000; i++ {
		if err := tryWriteFrame(conn, wire.Frame{Type: wire.TypePSH, StreamID: 1}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if sess.IsClosed() {
		t.Fatalf("session closed on zero-length no-op: %v", sess.closeError())
	}
	if n := sess.ctrlLen(); n != 0 {
		t.Fatalf("control queue = %d, want 0 for zero-length no-ops", n)
	}
}

func TestUnknownStreamPSHCoalescesRST(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())
	for i := 0; i < 2000; i++ {
		if err := tryWriteFrame(conn, wire.Frame{
			Type:     wire.TypePSH,
			StreamID: 1,
			Payload:  []byte{'x'},
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if sess.IsClosed() {
		t.Fatalf("session closed unexpectedly: %v", sess.closeError())
	}
	// One RST may be queued and one may be in flight; the rest are deduped.
	if n := sess.ctrlLen(); n > 2 {
		t.Fatalf("control queue = %d, want at most 2 for repeated RSTs", n)
	}
}

func TestPingFloodIsBounded(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())
	for i := 0; i < 10000; i++ {
		if err := tryWriteFrame(conn, wire.Frame{
			Type:     wire.TypePING,
			StreamID: uint32(i + 1),
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if sess.IsClosed() {
		t.Fatalf("session closed unexpectedly: %v", sess.closeError())
	}
	if n := sess.ctrlLen(); n > ctrlPongHighWater {
		t.Fatalf("control queue = %d, want <= %d", n, ctrlPongHighWater)
	}
}

func TestControlQueueOverflowClosesSession(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())
	for i := 0; i < ctrlQueueLimit+512; i++ {
		err := tryWriteFrame(conn, wire.Frame{
			Type:     wire.TypePSH,
			StreamID: uint32(i + 1),
			Payload:  []byte{'x'},
		})
		if err != nil {
			break
		}
	}
	waitFor(t, 2*time.Second, sess.IsClosed)
	if err := sess.closeError(); !errors.Is(err, ErrFlowControl) {
		t.Fatalf("close error = %v, want ErrFlowControl", err)
	}
	if n := sess.ctrlLen(); n > ctrlQueueLimit {
		t.Fatalf("control queue = %d, want <= %d", n, ctrlQueueLimit)
	}
}

func TestUnknownStreamBudgetChargesFrames(t *testing.T) {
	cfg := testConfig()
	cfg.MaxUnknownStreamDiscard = 1024
	sess, conn := newRawPeer(t, cfg)
	for i := 0; i < 20; i++ {
		err := tryWriteFrame(conn, wire.Frame{
			Type:     wire.TypePSH,
			StreamID: uint32(i + 1),
			Payload:  []byte{'x'},
		})
		if err != nil {
			break
		}
	}
	waitFor(t, 2*time.Second, sess.IsClosed)
	if err := sess.closeError(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("close error = %v, want ErrProtocol", err)
	}
}

func TestReadDataWithEOFIsDelivered(t *testing.T) {
	for _, batchSize := range []int{0, 8 << 10} {
		batchSize := batchSize
		name := "default"
		if batchSize != 0 {
			name = "batched"
		}
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			cfg.ReadBatchSize = batchSize
			c1, c2 := net.Pipe()
			sc := &eofAfterDataConn{Conn: c2}
			cli := newSession(cfg, c1, true)
			srv := newSession(cfg, sc, false)
			defer cli.Abort()
			defer srv.Abort()

			cs, ss := openTestStream(t, cli, srv)
			sc.inject.Store(true)
			_, _ = cs.Write([]byte("hello"))

			buf := make([]byte, 5)
			_ = ss.SetReadDeadline(time.Now().Add(time.Second))
			n, err := io.ReadFull(ss, buf)
			if n != len(buf) || err != nil {
				ss.bufferLock.Lock()
				ringLen := ss.ring.len()
				ss.bufferLock.Unlock()
				t.Fatalf("ReadFull = %d, %v; want 5, nil (ring=%d recv=%d consumed=%d drain=%v accepted=%v registered=%v sessClosed=%v closeErr=%v last=%x)",
					n, err, ringLen, ss.recvBytes.Load(), ss.consumed.Load(), ss.getDrainErr(), ss.accepted.Load(),
					srv.streams.get(ss.id) == ss, srv.IsClosed(), srv.closeError(), sc.last.Load())
			}
			if string(buf) != "hello" {
				t.Fatalf("read = %q, want hello", buf)
			}
			if _, err := ss.Read(buf); !errors.Is(err, io.EOF) {
				t.Fatalf("second Read = %v, want io.EOF", err)
			}
			waitFor(t, 2*time.Second, srv.IsClosed)
		})
	}
}

func TestSettingsCannotShrinkWindows(t *testing.T) {
	cfg := testConfig()
	c1, c2 := net.Pipe()
	sess := newSession(cfg, c1, false)
	defer sess.Abort()
	defer c2.Close()

	_ = readWireFrame(t, c2, time.Second) // server SETTINGS
	settings := wire.DefaultSettings()
	settings.InitialStreamWindow = 64 << 10
	settings.InitialSessionWindow = 128 << 10
	writeWireFrame(t, c2, wire.Frame{
		Type:    wire.TypeSETTINGS,
		Payload: mustSettingsPayload(t, settings),
	})

	waitFor(t, 2*time.Second, sess.IsClosed)
	if err := sess.closeError(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("close error = %v, want ErrProtocol", err)
	}
}

func mustSettingsPayload(t *testing.T, settings wire.Settings) []byte {
	t.Helper()
	p, err := wire.AppendSettings(nil, settings)
	if err != nil {
		t.Fatalf("AppendSettings: %v", err)
	}
	return p
}

func TestSessionClosedMarksUnacceptedStream(t *testing.T) {
	s := newWriteFramesSession(&fixedConn{})
	s.chAccepts = make(chan *Stream, 1)
	st := newStream(s, 1, false, 0)
	s.chAccepts <- st
	st.sessionClosed(ErrSessionClosed)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if got, err := s.AcceptStreamContext(ctx); got != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcceptStreamContext = (%v, %v), want (nil, context deadline)", got, err)
	}
}

func TestWriterIdleWaitsForSettings(t *testing.T) {
	s := newWriteFramesSession(&fixedConn{})
	if s.writerIdle() {
		t.Fatal("writerIdle returned true before SETTINGS was written")
	}
	s.settingsSent.Store(true)
	if !s.writerIdle() {
		t.Fatal("writerIdle returned false after SETTINGS was written")
	}
}

func TestWriteFramesMarksSettingsSent(t *testing.T) {
	settings, err := wire.NewSettingsFrame(wire.DefaultSettings())
	if err != nil {
		t.Fatalf("NewSettingsFrame: %v", err)
	}
	conn := &fixedConn{n: wire.HeaderSize + len(settings.Payload)}
	s := newWriteFramesSession(conn)
	var hdrArena []byte
	var vec [][]byte
	var infos []frameInfo
	if !s.writeFrames([]ctrlMsg{{frame: settings}}, nil, &hdrArena, &vec, &infos) {
		t.Fatal("writeFrames failed")
	}
	if !s.settingsSent.Load() {
		t.Fatal("settingsSent was not set after writing SETTINGS")
	}
}

func TestBufferedFrameCountReturnsToZero(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)
	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(ss, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		return srv.bufferedFrames.Load() == 0
	})
	_ = cs.Close()
	_ = ss.Close()
}

func TestSessionFrameBudgetIsBounded(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())
	ids := []uint32{1, 3, 5}
	for _, id := range ids {
		if err := tryWriteFrame(conn, wire.Frame{
			Type:     wire.TypePSH,
			Flags:    wire.FlagSYN,
			StreamID: id,
			Payload:  []byte{'a'},
		}); err != nil {
			t.Fatalf("open stream %d: %v", id, err)
		}
	}

	const perStream = 50 << 10
outer:
	for i := 0; i < perStream; i++ {
		for _, id := range ids {
			if err := tryWriteFrame(conn, wire.Frame{
				Type:     wire.TypePSH,
				StreamID: id,
				Payload:  []byte{'x'},
			}); err != nil {
				break outer
			}
		}
	}
	waitFor(t, 2*time.Second, sess.IsClosed)
	if err := sess.closeError(); !errors.Is(err, ErrFlowControl) {
		t.Fatalf("close error = %v, want ErrFlowControl", err)
	}
	if n := sess.bufferedFrames.Load(); n > maxSessionBufferedFrames {
		t.Fatalf("buffered frames = %d, want <= %d", n, maxSessionBufferedFrames)
	}
}

func TestSetWriteDeadlineWakesWaitCredit(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	waitSettings(t, cli)
	waitSettings(t, srv)
	cs, ss := openTestStream(t, cli, srv)
	cs.peerWindow.Store(0)

	done := make(chan error, 1)
	go func() {
		_, err := cs.Write([]byte("blocked"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := cs.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("Write = %v, want ErrTimeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write was not woken by SetWriteDeadline")
	}
	_ = cs.Close()
	_ = ss.Close()
}

func TestSetWriteDeadlineWakesAwaitBatch(t *testing.T) {
	s := newWriteFramesSession(&fixedConn{})
	st := newStream(s, 1, true, 0)
	b := newWriteBatch(st, []byte("x"), 0, getDoneChan())

	done := make(chan error, 1)
	go func() {
		_, err := st.awaitBatch(b, time.Time{}, true)
		done <- err
	}()

	st.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	deadline := time.Now().Add(time.Second)
	for !b.canceled.Load() {
		if time.Now().After(deadline) {
			t.Fatal("awaitBatch was not woken by SetWriteDeadline")
		}
		time.Sleep(time.Millisecond)
	}
	b.complete(0, ErrTimeout)

	select {
	case err := <-done:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("awaitBatch = %v, want ErrTimeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("awaitBatch did not return after cancellation")
	}
	b.release()
}

func TestManyTinyFramesAreBounded(t *testing.T) {
	cfg := testConfig()
	sess, conn := newRawPeer(t, cfg)
	if err := tryWriteFrame(conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN,
		StreamID: 1,
		Payload:  []byte{'a'},
	}); err != nil {
		t.Fatalf("open stream: %v", err)
	}
	st, err := sess.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	for i := 0; i < maxBufferedFrames+1024; i++ {
		if err := tryWriteFrame(conn, wire.Frame{
			Type:     wire.TypePSH,
			StreamID: 1,
			Payload:  []byte{'x'},
		}); err != nil {
			break
		}
	}
	waitFor(t, 2*time.Second, sess.IsClosed)
	if err := sess.closeError(); !errors.Is(err, ErrFlowControl) {
		t.Fatalf("close error = %v, want ErrFlowControl", err)
	}
	st.bufferLock.Lock()
	n := st.ring.len()
	st.bufferLock.Unlock()
	if n > maxBufferedFrames {
		t.Fatalf("buffered frames = %d, want <= %d", n, maxBufferedFrames)
	}
}
