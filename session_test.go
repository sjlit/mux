package mux

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

func testConfig() *Config {
	cfg := DefaultConfig()
	cfg.KeepAliveDisabled = true
	return cfg
}

func newTestPair(t *testing.T, cfg *Config) (*Session, *Session) {
	t.Helper()
	c1, c2 := net.Pipe()
	cli := newSession(cfg, c1, true)
	srv := newSession(cfg, c2, false)
	t.Cleanup(func() {
		_ = cli.Abort()
		_ = srv.Abort()
	})
	return cli, srv
}

func waitSettings(t *testing.T, sess *Session) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !sess.peerSettingsSet.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for SETTINGS")
		}
		time.Sleep(time.Millisecond)
	}
}

func openTestStream(t *testing.T, cli, srv *Session) (*Stream, *Stream) {
	t.Helper()
	cs, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	ss, err := srv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	return cs, ss
}

func TestEchoAndHalfClose(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(ss, buf); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("server read = %q, want hello", buf)
	}

	if _, err := ss.Write([]byte("world")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	if _, err := io.ReadFull(cs, buf); err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(buf) != "world" {
		t.Fatalf("client read = %q, want world", buf)
	}

	if err := cs.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
	if _, err := ss.Read(buf); err != io.EOF {
		t.Fatalf("server Read after FIN = %v, want EOF", err)
	}
	if err := ss.CloseWrite(); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	if _, err := cs.Read(buf); err != io.EOF {
		t.Fatalf("client Read after FIN = %v, want EOF", err)
	}

	_ = cs.Close()
	_ = ss.Close()
}

func TestCloseDeliversQueuedData(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	payload := bytes.Repeat([]byte("mux-v3"), 40000)

	type result struct {
		data []byte
		err  error
	}
	readDone := make(chan result, 1)
	go func() {
		var out bytes.Buffer
		buf := make([]byte, 16*1024)
		for {
			n, err := ss.Read(buf)
			if n > 0 {
				out.Write(buf[:n])
			}
			if err != nil {
				readDone <- result{data: out.Bytes(), err: err}
				return
			}
		}
	}()

	if _, err := cs.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case got := <-readDone:
		if got.err != io.EOF {
			t.Fatalf("reader error = %v, want EOF", got.err)
		}
		if !bytes.Equal(got.data, payload) {
			t.Fatalf("read %d bytes, want %d", len(got.data), len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for queued data")
	}
}

func TestReset(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	if err := cs.Reset(wire.CodeCancel); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	buf := make([]byte, 1)
	_ = ss.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := ss.Read(buf)
	if !errors.Is(err, ErrStreamReset) {
		t.Fatalf("Read after RST = %v, want ErrStreamReset", err)
	}

	// The session must remain usable after a stream reset.
	cs2, ss2 := openTestStream(t, cli, srv)
	if _, err := cs2.Write([]byte("ok")); err != nil {
		t.Fatalf("second stream write: %v", err)
	}
	if _, err := io.ReadFull(ss2, buf); err != nil || buf[0] != 'o' {
		t.Fatalf("second stream read = %q, %v", buf[:1], err)
	}
	_ = cs2.Close()
	_ = ss2.Close()
}

func TestFlowControlSmallWindow(t *testing.T) {
	cfg := testConfig()
	cfg.MaxFrameSize = 32768
	cfg.InitialStreamWindow = 64 << 10
	cfg.InitialSessionWindow = 256 << 10
	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	cs, ss := openTestStream(t, cli, srv)
	if got := cs.peerWindow.Load(); got != 64<<10 {
		t.Fatalf("client peer window = %d, want %d", got, 64<<10)
	}

	const total = 1 << 20
	payload := bytes.Repeat([]byte{0xab}, total)

	writeErr := make(chan error, 1)
	go func() {
		_, err := cs.Write(payload)
		writeErr <- err
	}()

	readDone := make(chan error, 1)
	go func() {
		got := 0
		buf := make([]byte, 16*1024)
		for got < total {
			n, err := ss.Read(buf)
			if err != nil {
				readDone <- err
				return
			}
			got += n
		}
		readDone <- nil
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reader timed out")
	}
	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("writer timed out")
	}

	_ = cs.Close()
	_ = ss.Close()
}

func TestReadDeadline(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)
	defer cs.Close()
	defer ss.Close()

	if err := cs.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := cs.Read(make([]byte, 1)); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Read = %v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("deadline took %v", d)
	}
}

func TestWriteDeadlineWhileBlocked(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	waitSettings(t, cli)
	waitSettings(t, srv)
	cs, ss := openTestStream(t, cli, srv)
	defer cs.Close()
	defer ss.Close()

	cs.peerWindow.Store(0) // no credit available
	if err := cs.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := cs.Write([]byte("blocked")); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Write = %v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("deadline took %v", d)
	}
}

func TestKeepAlivePingPong(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepAliveInterval = 20 * time.Millisecond
	cfg.KeepAliveTimeout = 500 * time.Millisecond
	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	deadline := time.Now().Add(2 * time.Second)
	for cli.lastRTT.Load() == 0 || srv.lastRTT.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for PONG")
		}
		if cli.IsClosed() || srv.IsClosed() {
			t.Fatal("session closed unexpectedly")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestKeepAliveTimeout(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()

	cfg := DefaultConfig()
	cfg.KeepAliveInterval = 20 * time.Millisecond
	cfg.KeepAliveTimeout = 80 * time.Millisecond
	sess := newSession(cfg, c1, true)
	defer sess.Abort()

	select {
	case <-sess.CloseChan():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not time out")
	}
}

func TestConcurrentStreams(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	const streams = 8

	accepted := make(chan *Stream, streams)
	acceptErr := make(chan error, 1)
	go func() {
		for i := 0; i < streams; i++ {
			st, err := srv.AcceptStream()
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- st
		}
	}()

	clients := make([]*Stream, 0, streams)
	for i := 0; i < streams; i++ {
		st, err := cli.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream %d: %v", i, err)
		}
		clients = append(clients, st)
	}

	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		select {
		case ss := <-accepted:
			wg.Add(1)
			go func(ss *Stream, id byte) {
				defer wg.Done()
				defer ss.Close()
				buf := make([]byte, 4096)
				n, err := io.ReadFull(ss, buf)
				if err != nil {
					t.Errorf("echo read: %v", err)
					return
				}
				if _, err := ss.Write(buf[:n]); err != nil {
					t.Errorf("echo write: %v", err)
				}
			}(ss, byte(i))
		case err := <-acceptErr:
			t.Fatalf("accept: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out accepting streams")
		}
	}

	for i, st := range clients {
		wg.Add(1)
		go func(st *Stream, id byte) {
			defer wg.Done()
			defer st.Close()
			msg := bytes.Repeat([]byte{id}, 4096)
			if _, err := st.Write(msg); err != nil {
				t.Errorf("client write: %v", err)
				return
			}
			buf := make([]byte, len(msg))
			if _, err := io.ReadFull(st, buf); err != nil {
				t.Errorf("client read: %v", err)
				return
			}
			if !bytes.Equal(buf, msg) {
				t.Errorf("stream %d echo mismatch", id)
			}
		}(st, byte(i))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent streams timed out")
	}
}

func TestCloseGracefulTimesOutWithOpenStream(t *testing.T) {
	cfg := testConfig()
	cfg.CloseTimeout = 100 * time.Millisecond
	cli, srv := newTestPair(t, cfg)
	_, _ = openTestStream(t, cli, srv)

	start := time.Now()
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v", d)
	}
	if !cli.IsClosed() {
		t.Fatal("session not closed")
	}
}
