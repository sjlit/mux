package mux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// segmentConn limits each Read to readMax bytes, emulating a packet-oriented
// transport such as KCP where a Read returns at most one segment.
type segmentConn struct {
	net.Conn
	readMax int
}

func (c *segmentConn) Read(p []byte) (int, error) {
	if len(p) > c.readMax {
		p = p[:c.readMax]
	}
	return c.Conn.Read(p)
}

func newSegmentedPair(tb testing.TB, cfg *Config, readMax int) (*Session, *Session) {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer ln.Close()

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := ln.Accept()
		accepted <- acceptResult{conn: conn, err: err}
	}()

	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	res := <-accepted
	if res.err != nil {
		tb.Fatal(res.err)
	}

	cli, err := Client(&segmentConn{Conn: c1, readMax: readMax}, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	srv, err := Server(&segmentConn{Conn: res.conn, readMax: readMax}, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		_ = cli.Abort()
		_ = srv.Abort()
	})

	deadline := time.Now().Add(2 * time.Second)
	for !cli.peerSettingsSet.Load() || !srv.peerSettingsSet.Load() {
		if time.Now().After(deadline) {
			tb.Fatal("timed out waiting for SETTINGS")
		}
		time.Sleep(time.Millisecond)
	}
	return cli, srv
}

func TestSegmentedMultiStream(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepAliveDisabled = true
	cfg.ReadBatchSize = 128 << 10
	cli, srv := newSegmentedPair(t, cfg, 1400)

	const streams = 4
	const messageSize = 1024
	const iterations = 64

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
			t.Fatalf("OpenStream: %v", err)
		}
		clients = append(clients, st)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, streams*2)
	for i := 0; i < streams; i++ {
		var ss *Stream
		select {
		case ss = <-accepted:
		case err := <-acceptErr:
			t.Fatalf("accept: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out accepting streams")
		}
		wg.Add(1)
		go func(ss *Stream) {
			defer wg.Done()
			defer ss.Close()
			buf := make([]byte, messageSize)
			for j := 0; j < iterations; j++ {
				if _, err := io.ReadFull(ss, buf); err != nil {
					errCh <- fmt.Errorf("echo read: %w", err)
					return
				}
				if _, err := ss.Write(buf); err != nil {
					errCh <- fmt.Errorf("echo write: %w", err)
					return
				}
			}
		}(ss)
	}

	for streamID, cs := range clients {
		wg.Add(1)
		go func(streamID int, cs *Stream) {
			defer wg.Done()
			defer cs.Close()
			msg := bytes.Repeat([]byte{byte(streamID)}, messageSize)
			buf := make([]byte, messageSize)
			for j := 0; j < iterations; j++ {
				msg[0] = byte(streamID)
				if _, err := cs.Write(msg); err != nil {
					errCh <- fmt.Errorf("client write: %w", err)
					return
				}
				if _, err := io.ReadFull(cs, buf); err != nil {
					errCh <- fmt.Errorf("client read: %w", err)
					return
				}
				if !bytes.Equal(buf, msg) {
					errCh <- errors.New("payload mismatch")
					return
				}
			}
		}(streamID, cs)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("segmented multi-stream test timed out")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
}

func benchmarkSegmented(b *testing.B, size int, batch bool) {
	cfg := DefaultConfig()
	cfg.KeepAliveDisabled = true
	if batch {
		cfg.ReadBatchSize = 128 << 10
	}
	cli, srv := newSegmentedPair(b, cfg, 1400)

	cs, err := cli.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	ss, err := srv.AcceptStream()
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()

	payload := make([]byte, size)
	rbuf := make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got := 0
		for got < size*b.N {
			n, err := ss.Read(rbuf)
			if err != nil {
				b.Error(err)
				return
			}
			got += n
		}
	}()
	for i := 0; i < b.N; i++ {
		if _, err := cs.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
}

func BenchmarkSegmentedThroughput(b *testing.B) {
	cases := []struct {
		name  string
		size  int
		batch bool
	}{
		{"large_unbatched", 32 << 10, false},
		{"large_batched", 32 << 10, true},
		{"small_unbatched", 1 << 10, false},
		{"small_batched", 1 << 10, true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkSegmented(b, tc.size, tc.batch)
		})
	}
}
