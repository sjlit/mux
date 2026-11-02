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

func testConfigBatched() *Config {
	cfg := testConfig()
	cfg.ReadBatchSize = 128 << 10
	return cfg
}

func TestBatchedEchoAndHalfClose(t *testing.T) {
	cli, srv := newTestPair(t, testConfigBatched())
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
	_ = cs.Close()
	_ = ss.Close()
}

func TestBatchedLargePayload(t *testing.T) {
	cli, srv := newTestPair(t, testConfigBatched())
	cs, ss := openTestStream(t, cli, srv)

	const total = 4 << 20
	payload := make([]byte, total)
	for i := range payload {
		payload[i] = byte(i*17 + i>>7)
	}

	writeErr := make(chan error, 1)
	go func() {
		_, err := cs.Write(payload)
		writeErr <- err
	}()

	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 32*1024)
		got := 0
		for got < total {
			n, err := ss.Read(buf)
			if err != nil {
				readErr <- err
				return
			}
			if !bytes.Equal(buf[:n], payload[got:got+n]) {
				readErr <- errors.New("payload mismatch")
				return
			}
			got += n
		}
		readErr <- nil
	}()

	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("write timed out")
	}
	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("read timed out")
	}
	_ = cs.Close()
	_ = ss.Close()
}

type chunkReadConn struct {
	net.Conn
	max int
}

func (c *chunkReadConn) Read(p []byte) (int, error) {
	if len(p) > c.max {
		p = p[:c.max]
	}
	return c.Conn.Read(p)
}

func TestBatchedChunkedReads(t *testing.T) {
	cfg := testConfigBatched()
	c1, c2 := net.Pipe()
	cli := newSession(cfg, c1, true)
	srv := newSession(cfg, &chunkReadConn{Conn: c2, max: 7}, false)
	t.Cleanup(func() {
		_ = cli.Abort()
		_ = srv.Abort()
	})

	cs, ss := openTestStream(t, cli, srv)
	payload := bytes.Repeat([]byte("chunked-read-"), 8*1024)

	writeErr := make(chan error, 1)
	go func() {
		_, err := cs.Write(payload)
		writeErr <- err
	}()

	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(ss, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("payload mismatch")
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = cs.Close()
	_ = ss.Close()
}

func TestBatchedFlowControl(t *testing.T) {
	cfg := testConfigBatched()
	cfg.InitialStreamWindow = 64 << 10
	cfg.InitialSessionWindow = 256 << 10
	cli, srv := newTestPair(t, cfg)
	waitSettings(t, cli)
	waitSettings(t, srv)

	cs, ss := openTestStream(t, cli, srv)
	const total = 1 << 20
	payload := bytes.Repeat([]byte{0xcd}, total)

	writeErr := make(chan error, 1)
	go func() {
		_, err := cs.Write(payload)
		writeErr <- err
	}()
	readErr := make(chan error, 1)
	go func() {
		got := 0
		buf := make([]byte, 16*1024)
		for got < total {
			n, err := ss.Read(buf)
			if err != nil {
				readErr <- err
				return
			}
			got += n
		}
		readErr <- nil
	}()

	select {
	case err := <-readErr:
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

func TestBatchedReset(t *testing.T) {
	cli, srv := newTestPair(t, testConfigBatched())
	cs, ss := openTestStream(t, cli, srv)

	if err := cs.Reset(wire.CodeCancel); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	buf := make([]byte, 1)
	_ = ss.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := ss.Read(buf); !errors.Is(err, ErrStreamReset) {
		t.Fatalf("Read after RST = %v, want ErrStreamReset", err)
	}
}

func TestBatchedUnknownStream(t *testing.T) {
	cfg := testConfigBatched()
	sess, conn := newRawPeer(t, cfg)

	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		StreamID: 1,
		Payload:  bytes.Repeat([]byte{0xaa}, 4096),
	})
	readUntil(t, conn, func(f wire.Frame) bool {
		return f.Type == wire.TypeRST && f.StreamID == 1
	})

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

func TestBatchedTCP(t *testing.T) {
	cs, ss := newBenchPair(t, func(c *Config) {
		c.ReadBatchSize = 128 << 10
	})
	defer cs.Close()
	defer ss.Close()

	const chunk = 128 << 10
	const ops = 64
	total := chunk * ops
	payload := bytes.Repeat([]byte{0x5a}, chunk)

	var got atomic.Int64
	readErr := make(chan error, 1)
	go func() {
		rbuf := make([]byte, chunk)
		for int(got.Load()) < total {
			n, err := ss.Read(rbuf)
			if err != nil {
				readErr <- err
				return
			}
			for i := 0; i < n; i++ {
				if rbuf[i] != 0x5a {
					readErr <- errors.New("payload corruption")
					return
				}
			}
			got.Add(int64(n))
			if got.Load()%(1<<20) < int64(n) {
				t.Logf("received %d/%d", got.Load(), total)
			}
		}
		readErr <- nil
	}()

	writeErr := make(chan error, 1)
	go func() {
		for i := 0; i < ops; i++ {
			if _, err := cs.Write(payload); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	for {
		select {
		case err := <-readErr:
			if err != nil {
				t.Fatalf("read: %v (got %d/%d)", err, got.Load(), total)
			}
			return
		case err := <-writeErr:
			if err != nil {
				t.Fatalf("write: %v (got %d/%d)", err, got.Load(), total)
			}
		case <-time.After(15 * time.Second):
			t.Logf("writer: numWritten=%d reserved=%d peerConsumed=%d window=%d",
				cs.numWritten.Load(), cs.reserved.Load(), cs.peerConsumed.Load(), cs.peerWindow.Load())
			t.Logf("reader: recvBytes=%d consumed=%d ringEmpty=%v window=%d",
				ss.recvBytes.Load(), ss.consumed.Load(), ss.ringEmpty(), ss.recvWindow)
			t.Logf("session: numWritten=%d reserved=%d peerConsumed=%d window=%d recv=%d consumed=%d window=%d",
				cs.sess.sessionNumWritten.Load(), cs.sess.sessionReserved.Load(),
				cs.sess.peerSessionConsumed.Load(), cs.sess.peerSessionWindow.Load(),
				ss.sess.sessionRecvBytes.Load(), ss.sess.sessionConsumed.Load(), ss.sess.recvSessionWindow)
			t.Fatalf("timed out at %d/%d bytes", got.Load(), total)
		}
	}
}

func TestBatchedAdaptiveSmallFrames(t *testing.T) {
	cfg := testConfigBatched()
	c1, c2 := net.Pipe()
	cli := newSession(cfg, c1, true)
	srv := newSession(cfg, &chunkReadConn{Conn: c2, max: 1400}, false)
	t.Cleanup(func() {
		_ = cli.Abort()
		_ = srv.Abort()
	})

	cs, ss := openTestStream(t, cli, srv)
	const frameSize = 1024
	const count = 500
	payload := bytes.Repeat([]byte{0x33}, frameSize)

	writeErr := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			if _, err := cs.Write(payload); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	got := make([]byte, frameSize*count)
	if _, err := io.ReadFull(ss, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat(payload, count)) {
		t.Fatal("payload mismatch")
	}
	if allocs := srv.blockPool.large.allocs.Load(); allocs != 0 {
		t.Fatalf("large block allocated %d times for 1 KiB frames", allocs)
	}
	_ = cs.Close()
	_ = ss.Close()
}

func TestBatchedAdaptiveGrows(t *testing.T) {
	cfg := testConfigBatched()
	c1, c2 := net.Pipe()
	cli := newSession(cfg, c1, true)
	srv := newSession(cfg, &chunkReadConn{Conn: c2, max: 1400}, false)
	t.Cleanup(func() {
		_ = cli.Abort()
		_ = srv.Abort()
	})

	cs, ss := openTestStream(t, cli, srv)
	big := bytes.Repeat([]byte{0x44}, 32*1024)
	small := bytes.Repeat([]byte{0x55}, 1024)
	want := append(append([]byte(nil), big...), small...)

	writeErr := make(chan error, 1)
	go func() {
		if _, err := cs.Write(big); err != nil {
			writeErr <- err
			return
		}
		if _, err := cs.Write(small); err != nil {
			writeErr <- err
			return
		}
		writeErr <- nil
	}()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(ss, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("payload mismatch")
	}
	if allocs := srv.blockPool.large.allocs.Load(); allocs == 0 {
		t.Fatal("large block was not allocated for the 32 KiB frame")
	}
	_ = cs.Close()
	_ = ss.Close()
}

func TestBatchedKeepAlive(t *testing.T) {
	cfg := testConfigBatched()
	cfg.KeepAliveDisabled = false
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
