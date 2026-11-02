# mux

mux is a multiplexing library for Go built on the next-generation smux wire
protocol (version 3). It is derived from the v3 work in
[`xtaci/smux`](https://github.com/xtaci/smux).

- [`PROTOCOL.md`](PROTOCOL.md): normative wire protocol specification.
- [`wire`](wire): stateless frame codec (decode/encode, zero-copy payloads).
- `.`: Stage 1 runtime (`Session`, `Stream`, SETTINGS handshake, credit flow
  control, batched writev, SYN/FIN flags, RST, GOAWAY, PING/PONG).

## Usage

```go
import "github.com/sjlit/mux"

sess, err := mux.Client(conn, nil)
if err != nil {
    return err
}
defer sess.Close()

stream, err := sess.OpenStream()
if err != nil {
    return err
}
defer stream.Close()

_, err = stream.Write([]byte("hello"))
```

The server side uses `mux.Server(conn, nil)` and `AcceptStream`.

## Runtime defaults

- `MaxFrameSize`: 65535 (SETTINGS negotiate the effective value per peer)
- `WriteBatchSize`: 128 KiB
- `WritePipeline`: 4
- `ReadBatchSize`: 0 (opt-in block reader; the default reader already batches)
- stream window 1 MiB, session window 8 MiB
- one reader goroutine, one writer goroutine, optional keepalive goroutine

The write path collects the frames of one `Write` into one burst. When the
writer goroutine is idle, the caller writes the burst directly with a single
`writev`; otherwise the burst is queued for the fair round-robin writer. This
keeps request/response traffic off the writer goroutine while preserving
ordering and flow control.

The default receive path uses a 4 KiB read-ahead buffer. A small frame usually
arrives with one `read`; a larger frame copies the buffered prefix into its
pooled payload buffer and reads the remainder directly. Complete frames left in
the buffer are parsed before the next read, and one application `Read` can
coalesce several queued frames.

`ReadBatchSize` enables an alternative refcounted block reader that parses
several frames from one large syscall. Small frames use 8 KiB blocks, and a
frame larger than that grows the reader to a `ReadBatchSize` block on demand.
With the read-ahead path above, it is approximately neutral in the benchmarks
below, so it remains opt-in.

## Limits and caveats

- A stream queues at most 64K discrete received payloads and a session at most
  128K across all streams. Flow control bounds bytes, but a peer sending
  one-byte frames would otherwise create one ring entry per byte; exceeding a
  cap closes the session with `ErrFlowControl`.
- Control frames are bounded (4K entries); repeated RSTs for the same stream
  are coalesced and PONGs are dropped above a high-water mark during a PING
  flood.
- A write deadline cannot interrupt a `conn.Write` that is already in progress.
  A blocked write is completed or the underlying connection must be closed
  (`Session.Abort`/`Session.Close`) to unblock it.
- After a transport read error, accepted streams drain already-buffered payloads
  before returning the error, but a hard protocol or flow-control violation
  still aborts the session immediately.

## Performance (Apple M2, loopback TCP, 128 KiB writes)

| Implementation | Time | Throughput | Allocations |
|---|---|---|---|
| upstream `smux` v1/v2 (32K frames) | 31.0 µs | 4.23 GB/s | 24 allocs/op |
| v3 default (64K frames) | 14.9 µs | 8.79 GB/s | 0 |
| v3 256K write batch | 14.8 µs | 8.86 GB/s | 0 |
| v3 `ReadBatchSize=128K` | 15.3 µs | 8.57 GB/s | 0 |
| raw TCP | 8.4 µs | 15.6 GB/s | 0 |

Small messages and segmented transports:

| Scenario | Time / Throughput |
|---|---|
| 1 KiB ping-pong | 24.0 µs |
| 1 KiB stream writes, TCP | 2.05 µs / 500 MB/s |
| 32 KiB frames, 1400-byte reads (KCP-like) | 2.04 GB/s |
| 1 KiB frames, 1400-byte reads (KCP-like) | 280 MB/s |

Reproduce:

```bash
go test . -run='^$' -bench='BenchmarkStream|BenchmarkSmall|BenchmarkSegmented' -benchmem
go test ./wire -run='^$' -bench=. -benchmem
```

## License

MIT. The original smux copyright notice is retained in [`LICENSE`](LICENSE);
mux is derived from the smux v3 work.
