# AGENTS.md - mux Project Context

## Project Overview

**mux** is a multiplexing library for Go built on the next-generation smux
wire protocol (version 3). It is derived from the smux v3 work in
[`uole/smux`](https://github.com/uole/smux), which is based on
[`xtaci/smux`](https://github.com/xtaci/smux).

Module path: `github.com/sjlit/mux`.
The project is versioned from `v0.x`; the wire protocol version is fixed at 3
and is independent of the module version.

## Architecture

### `Session` (`session.go`)

Manages the underlying `io.ReadWriteCloser`, stream creation/acceptance, the
session credit window, keepalive, and the control plane (SETTINGS, GOAWAY,
PING/PONG, RST).

### `Stream` (`stream.go`)

Implements `net.Conn` over a logical stream, including per-stream credit flow
control, buffering, deadlines, and half-close semantics.

### Write path (`write.go`)

Collects the frames of one `Write` into a burst. When the writer goroutine is
idle, the caller writes the burst directly with a single `writev`; otherwise
the burst is queued for the fair round-robin writer.

### Wire codec (`wire/`)

Stateless frame codec. `DecodeFrame` parses at most one frame from the front of
a byte slice and reports how many bytes it consumed, so callers can feed it
arbitrarily fragmented or coalesced transport reads.

### Other files

- `block.go`, `bufferpool.go`: pooled read blocks and buffers.
- `notifier.go`: generation-based broadcast wakeups.
- `stream_table.go`: concurrent stream ID table.
- `errors.go`: protocol and runtime errors.

## Wire Protocol

See [`PROTOCOL.md`](PROTOCOL.md). Summary:

- 8-byte header: Version (1), Type/Flags (1), Length (2), Stream ID (4),
  optional 4-byte credit, then payload.
- Types: SETTINGS, SYN, PSH, FIN, RST, GOAWAY, PING, PONG.
- SETTINGS handshake negotiates max frame size, stream/session windows,
  keepalive, timeout, and max concurrent streams.
- Credit-based flow control uses absolute consumed counters.
- Wire version 3 is intentionally incompatible with smux v1/v2.

## Development Guidelines

### Testing

Go 1.27+ is required.

```bash
go test -race -short ./...
go test -race ./...
GOOS=linux GOARCH=386 go test ./...
go test -v -run=^$ -bench .
```

### Coding Conventions

- Follow standard Go coding conventions.
- Keep the wire format stable; extend with new frame types instead of changing
  existing layouts.
- `Session` and `Stream` are heavily concurrent; check lock ordering and avoid
  blocking while holding locks.
- A caller must not return from `Write` while a started request still
  references its payload.
- Data-frame admission and FIN submission share the per-stream gate; never
  hold that gate while waiting for `conn.Write`.
- Do not log payloads. Error messages should contain only type, stream ID,
  length, and error code.

## File Structure

- `session.go`, `stream.go`, `write.go`: runtime.
- `wire/frame.go`, `wire/settings.go`: wire codec.
- `PROTOCOL.md`: normative protocol specification.
- `*_test.go`: unit, integration, fuzz, and benchmark tests.
