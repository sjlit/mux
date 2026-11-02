package mux

import (
	"errors"
	"fmt"
	"net"

	"github.com/sjlit/mux/wire"
)

var (
	// ErrSessionClosed is returned after the session has shut down.
	ErrSessionClosed = errors.New("mux: session closed")
	// ErrStreamReset is returned when the peer resets a stream.
	ErrStreamReset = errors.New("mux: stream reset")
	// ErrStreamClosed is returned after a stream has been fully closed.
	ErrStreamClosed = errors.New("mux: stream closed")
	// ErrWriteClosed is returned when writing to a half-closed stream.
	ErrWriteClosed = errors.New("mux: write side closed")
	// ErrRefusedStream is returned when a stream could not be accepted.
	ErrRefusedStream = errors.New("mux: stream refused")
	// ErrGoAway is returned when the session is shutting down gracefully.
	ErrGoAway = errors.New("mux: session is going away")
	// ErrStreamIDExhausted is returned when no stream IDs remain.
	ErrStreamIDExhausted = errors.New("mux: stream id exhausted")
	// ErrProtocol is returned for wire-protocol violations.
	ErrProtocol = errors.New("mux: protocol error")
	// ErrFlowControl is returned when a peer exceeds an advertised window.
	ErrFlowControl = errors.New("mux: flow control error")
	// ErrKeepAliveTimeout is returned when no frame arrives in time.
	ErrKeepAliveTimeout = errors.New("mux: keepalive timeout")
	// ErrWouldBlock signals that a non-blocking operation has no data.
	ErrWouldBlock = errors.New("mux: would block")
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "mux: timeout" }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Timeout() bool   { return true }

// ErrTimeout is returned when a read, write, open or accept deadline expires.
var ErrTimeout net.Error = timeoutError{}

// StreamError describes a stream-level failure carrying a wire error code.
type StreamError struct {
	ID   uint32
	Code wire.ErrorCode
	Err  error
}

func (e *StreamError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("mux: stream %d: %s: %v", e.ID, e.Code, e.Err)
	}
	return fmt.Sprintf("mux: stream %d: %s", e.ID, e.Code)
}

func (e *StreamError) Unwrap() error { return e.Err }

func newStreamError(id uint32, code wire.ErrorCode) error {
	var base error
	switch code {
	case wire.CodeCancel:
		base = ErrStreamReset
	case wire.CodeRefusedStream:
		base = ErrRefusedStream
	case wire.CodeFlowControlError:
		base = ErrFlowControl
	case wire.CodeProtocolError, wire.CodeFrameSizeError:
		base = ErrProtocol
	default:
		base = ErrStreamClosed
	}
	return &StreamError{ID: id, Code: code, Err: base}
}

func itimediff(later, earlier uint32) int32 {
	return int32(later - earlier)
}
