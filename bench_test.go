package mux

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func newBenchPair(tb testing.TB, tune func(*Config)) (*Stream, *Stream) {
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

	cfg := DefaultConfig()
	cfg.KeepAliveDisabled = true
	if tune != nil {
		tune(cfg)
	}
	srv, err := Server(res.conn, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	cli, err := Client(c1, cfg)
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

	cs, err := cli.OpenStream()
	if err != nil {
		tb.Fatal(err)
	}
	ss, err := srv.AcceptStream()
	if err != nil {
		tb.Fatal(err)
	}
	return cs, ss
}

func benchmarkThroughput(b *testing.B, tune func(*Config)) {
	cs, ss := newBenchPair(b, tune)
	defer cs.Close()
	defer ss.Close()

	const size = 128 * 1024
	buf := make([]byte, size)
	rbuf := make([]byte, size)
	b.SetBytes(size)
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
		if _, err := cs.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
}

func BenchmarkStreamThroughput(b *testing.B) {
	cases := []struct {
		name string
		tune func(*Config)
	}{
		{"default", nil},
		{"batch128k", func(c *Config) { c.WriteBatchSize = 128 << 10 }},
		{"batch256k", func(c *Config) { c.WriteBatchSize = 256 << 10 }},
		{"frame64k", func(c *Config) { c.MaxFrameSize = 65535 }},
		{"depth8", func(c *Config) { c.WritePipeline = 8 }},
		{"f64b128d4", func(c *Config) {
			c.MaxFrameSize = 65535
			c.WriteBatchSize = 128 << 10
		}},
		{"f64b128d8", func(c *Config) {
			c.MaxFrameSize = 65535
			c.WriteBatchSize = 128 << 10
			c.WritePipeline = 8
		}},
		{"f64b256d4", func(c *Config) {
			c.MaxFrameSize = 65535
			c.WriteBatchSize = 256 << 10
		}},
		{"f32b128d8", func(c *Config) {
			c.WriteBatchSize = 128 << 10
			c.WritePipeline = 8
		}},
		{"batched", func(c *Config) {
			c.ReadBatchSize = 128 << 10
		}},
		{"batched256", func(c *Config) {
			c.ReadBatchSize = 128 << 10
			c.WriteBatchSize = 256 << 10
		}},
		{"batched_tuned", func(c *Config) {
			c.ReadBatchSize = 128 << 10
			c.WriteBatchSize = 256 << 10
			c.WritePipeline = 8
		}},
		{"tuned", func(c *Config) {
			c.MaxFrameSize = 65535
			c.WriteBatchSize = 256 << 10
			c.WritePipeline = 8
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkThroughput(b, tc.tune)
		})
	}
}

func BenchmarkStreamPingPong(b *testing.B) {
	cs, ss := newBenchPair(b, nil)
	defer cs.Close()
	defer ss.Close()

	const size = 1024
	buf := make([]byte, size)
	rbuf := make([]byte, size)
	b.SetBytes(size * 2)
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if _, err := io.ReadFull(ss, rbuf); err != nil {
				b.Error(err)
				return
			}
			if _, err := ss.Write(rbuf); err != nil {
				b.Error(err)
				return
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		if _, err := cs.Write(buf); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(cs, rbuf); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
}

func benchmarkSmallFrames(b *testing.B, tune func(*Config)) {
	cs, ss := newBenchPair(b, tune)
	defer cs.Close()
	defer ss.Close()

	const size = 1024
	buf := make([]byte, size)
	rbuf := make([]byte, size)
	b.SetBytes(size)
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
		if _, err := cs.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
}

func BenchmarkSmallFrames(b *testing.B) {
	cases := []struct {
		name string
		tune func(*Config)
	}{
		{"unbatched", nil},
		{"batched", func(c *Config) { c.ReadBatchSize = 128 << 10 }},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkSmallFrames(b, tc.tune)
		})
	}
}
