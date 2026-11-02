package mux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLargePayloadIntegrity(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	const total = 4 << 20
	payload := make([]byte, total)
	for i := range payload {
		payload[i] = byte(i*31 + i>>8)
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

func TestResetBeforeAccept(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())

	first, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return srv.streams.get(first.ID()) != nil
	})

	if err := first.Reset(wire.CodeCancel); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return srv.streams.get(first.ID()) == nil
	})

	second, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("second OpenStream: %v", err)
	}
	accepted, err := srv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if accepted.ID() != second.ID() {
		t.Fatalf("accepted stream %d, want %d (reset stream should be skipped)", accepted.ID(), second.ID())
	}

	if _, err := second.Write([]byte("alive")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(accepted, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "alive" {
		t.Fatalf("read = %q, want alive", buf)
	}
	_ = second.Close()
	_ = accepted.Close()
}

func TestOpenStreamContextCancel(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	waitSettings(t, cli)
	waitSettings(t, srv)

	cli.peerMaxStreams.Store(1)
	first, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("first OpenStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := cli.OpenStreamContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStreamContext = %v, want context deadline exceeded", err)
	}

	_ = first.Close()
}

func TestAcceptStreamContextCancel(t *testing.T) {
	_, srv := newTestPair(t, testConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := srv.AcceptStreamContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcceptStreamContext = %v, want context deadline exceeded", err)
	}
}

func TestSessionCloseIdempotent(t *testing.T) {
	cli, _ := newTestPair(t, testConfig())

	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := cli.Abort(); err != nil {
		t.Fatalf("Abort after Close: %v", err)
	}
	if !cli.IsClosed() {
		t.Fatal("session not closed")
	}
}

func TestStreamCloseIdempotent(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_ = ss.Close()
}
