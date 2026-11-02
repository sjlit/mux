package mux

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/sjlit/mux/wire"
)

func TestReadCoalescesFrames(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	const frameSize = 1024
	const frames = 4
	payload := make([]byte, frameSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := 0; i < frames; i++ {
		if _, err := cs.Write(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	time.Sleep(20 * time.Millisecond)

	buf := make([]byte, frameSize*frames)
	n, err := ss.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("coalesced read = %d bytes, want %d", n, len(buf))
	}
	for i := 0; i < frames; i++ {
		if string(buf[i*frameSize:(i+1)*frameSize]) != string(payload) {
			t.Fatalf("frame %d mismatch", i)
		}
	}
}

func TestLargeFrameWithCredit(t *testing.T) {
	sess, conn := newRawPeer(t, testConfig())

	payload := bytes.Repeat([]byte{0xab}, wire.MaxPayloadSize)
	writeWireFrame(t, conn, wire.Frame{
		Type:     wire.TypePSH,
		Flags:    wire.FlagSYN | wire.FlagCredit,
		StreamID: 1,
		Credit:   0,
		Payload:  payload,
	})

	st, err := sess.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestConcurrentReadersSeeEOF(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, ss := openTestStream(t, cli, srv)

	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			buf := make([]byte, 16)
			n, err := ss.Read(buf)
			results <- result{data: buf[:n], err: err}
		}()
	}
	time.Sleep(20 * time.Millisecond)

	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cs.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	var got []byte
	for i := 0; i < 2; i++ {
		select {
		case r := <-results:
			if r.err != nil && !errors.Is(r.err, io.EOF) {
				t.Fatalf("reader: %v", r.err)
			}
			got = append(got, r.data...)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent readers")
		}
	}
	if len(got) != 5 {
		t.Fatalf("readers got %d bytes (%q), want 5", len(got), got)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if string(got) != "ehllo" {
		t.Fatalf("readers got %q, want hello", got)
	}

	// Every reader must observe EOF, not just the first one.
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() {
			_, err := ss.Read(make([]byte, 1))
			done <- err
		}()
		select {
		case err := <-done:
			if err != io.EOF {
				t.Fatalf("read after FIN = %v, want EOF", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for EOF")
		}
	}
}

func TestSetReadDeadlineWakesConcurrentReaders(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	_, ss := openTestStream(t, cli, srv)

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := ss.Read(make([]byte, 1))
			errs <- err
		}()
	}
	time.Sleep(20 * time.Millisecond)
	if err := ss.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, ErrTimeout) {
				t.Fatalf("read = %v, want ErrTimeout", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for deadline")
		}
	}
}

func TestDirectWriteScratchDoesNotRetainPayloads(t *testing.T) {
	cli, srv := newTestPair(t, testConfig())
	cs, _ := openTestStream(t, cli, srv)

	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i, b := range cs.sess.directVec {
		if b != nil {
			t.Fatalf("direct vector %d retains %d bytes", i, len(b))
		}
	}
	for i, info := range cs.sess.directInfos {
		if info.batch != nil {
			t.Fatalf("direct info %d retains a batch", i)
		}
	}
}
