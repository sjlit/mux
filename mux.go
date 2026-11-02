package mux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/sjlit/mux/wire"
)

// Config tunes a v3 session. The zero value is not valid; use DefaultConfig.
type Config struct {
	// MaxFrameSize is both the largest PSH payload this session accepts and
	// the upper bound for outgoing frames. It must be in
	// [wire.DefaultMaxFrameSize, wire.MaxPayloadSize].
	MaxFrameSize int

	// InitialStreamWindow is the per-stream receive window advertised to the
	// peer. It must be at least wire.DefaultInitialStreamWindow.
	InitialStreamWindow int

	// InitialSessionWindow is the session-wide receive window advertised to
	// the peer. It must be at least wire.DefaultInitialSessionWindow.
	InitialSessionWindow int

	// MaxConcurrentStreams limits how many streams the peer may open. Use
	// wire.UnlimitedStreams to disable the limit, or 0 to refuse new streams.
	MaxConcurrentStreams uint32

	// AcceptBacklog bounds the number of accepted-but-not-yet-accepted streams.
	AcceptBacklog int

	// KeepAliveDisabled disables PING/PONG based liveness checking.
	KeepAliveDisabled bool
	// KeepAliveInterval is how often a PING is sent.
	KeepAliveInterval time.Duration
	// KeepAliveTimeout closes the session when no frame arrives in time.
	KeepAliveTimeout time.Duration

	// MaxUnknownStreamDiscard bounds payload bytes plus per-frame overhead
	// accepted for unknown or closed streams. Zero selects
	// max(1 MiB, 8 * InitialSessionWindow).
	MaxUnknownStreamDiscard int

	// WriteBatchSize is the soft byte budget for one writev batch. A single
	// frame is always allowed even when it exceeds this budget.
	WriteBatchSize int

	// WritePipeline is the number of data frames a single Write may keep in
	// flight before waiting for the oldest completion.
	WritePipeline int

	// ReadBatchSize enables the refcounted block reader. When non-zero, the
	// receiver reads up to this many bytes per syscall and parses multiple
	// frames from one block. Zero keeps the default read-ahead path, which
	// already coalesces small frames. Values below the largest possible frame
	// are raised to fit one frame.
	ReadBatchSize int

	// CloseTimeout bounds the graceful GOAWAY drain in Close.
	CloseTimeout time.Duration
	// OpenTimeout bounds OpenStream when the context has no deadline.
	OpenTimeout time.Duration
}

// DefaultConfig returns the default v3 configuration.
func DefaultConfig() *Config {
	return &Config{
		MaxFrameSize:         65535,
		InitialStreamWindow:  int(wire.DefaultInitialStreamWindow),
		InitialSessionWindow: int(wire.DefaultInitialSessionWindow),
		MaxConcurrentStreams: wire.UnlimitedStreams,
		AcceptBacklog:        1024,
		KeepAliveInterval:    10 * time.Second,
		KeepAliveTimeout:     30 * time.Second,
		WriteBatchSize:       128 << 10,
		WritePipeline:        4,
		ReadBatchSize:        0,
		CloseTimeout:         5 * time.Second,
		OpenTimeout:          30 * time.Second,
	}
}

// VerifyConfig validates a v3 configuration.
func VerifyConfig(config *Config) error {
	if config == nil {
		return errors.New("mux: nil config")
	}
	if config.MaxFrameSize < int(wire.DefaultMaxFrameSize) || config.MaxFrameSize > wire.MaxPayloadSize {
		return fmt.Errorf("mux: max frame size must be in [%d, %d]", wire.DefaultMaxFrameSize, wire.MaxPayloadSize)
	}
	if config.InitialStreamWindow < int(wire.DefaultInitialStreamWindow) || uint64(config.InitialStreamWindow) > uint64(wire.MaxWindow) {
		return fmt.Errorf("mux: initial stream window must be in [%d, %d]", wire.DefaultInitialStreamWindow, wire.MaxWindow)
	}
	if config.InitialSessionWindow < int(wire.DefaultInitialSessionWindow) || uint64(config.InitialSessionWindow) > uint64(wire.MaxWindow) {
		return fmt.Errorf("mux: initial session window must be in [%d, %d]", wire.DefaultInitialSessionWindow, wire.MaxWindow)
	}
	if config.InitialSessionWindow < config.InitialStreamWindow {
		return errors.New("mux: initial session window must be >= initial stream window")
	}
	if config.AcceptBacklog <= 0 {
		return errors.New("mux: accept backlog must be positive")
	}
	if !config.KeepAliveDisabled {
		if config.KeepAliveInterval <= 0 {
			return errors.New("mux: keepalive interval must be positive")
		}
		if config.KeepAliveTimeout < config.KeepAliveInterval {
			return errors.New("mux: keepalive timeout must be >= keepalive interval")
		}
	}
	if config.MaxUnknownStreamDiscard < 0 {
		return errors.New("mux: max unknown stream discard must not be negative")
	}
	if config.WriteBatchSize <= 0 || config.WriteBatchSize > 1<<20 {
		return errors.New("mux: write batch size must be in (0, 1 MiB]")
	}
	if config.WritePipeline <= 0 || config.WritePipeline > 64 {
		return errors.New("mux: write pipeline must be in [1, 64]")
	}
	if config.ReadBatchSize < 0 || config.ReadBatchSize > 1<<20 {
		return errors.New("mux: read batch size must be in [0, 1 MiB]")
	}
	if config.CloseTimeout <= 0 {
		return errors.New("mux: close timeout must be positive")
	}
	if config.OpenTimeout <= 0 {
		return errors.New("mux: open timeout must be positive")
	}
	return nil
}

// Server creates a server-side session. The server uses even stream IDs.
func Server(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	if conn == nil {
		return nil, errors.New("mux: nil connection")
	}
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, false), nil
}

// Client creates a client-side session. The client uses odd stream IDs.
func Client(conn io.ReadWriteCloser, config *Config) (*Session, error) {
	if conn == nil {
		return nil, errors.New("mux: nil connection")
	}
	if config == nil {
		config = DefaultConfig()
	}
	if err := VerifyConfig(config); err != nil {
		return nil, err
	}
	return newSession(config, conn, true), nil
}

// Dial connects to addr and returns a client-side session.
func Dial(network, addr string, config *Config) (*Session, error) {
	return DialContext(context.Background(), network, addr, config)
}

// DialContext is like Dial but uses ctx for the dial operation.
func DialContext(ctx context.Context, network, addr string, config *Config) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	sess, err := Client(conn, config)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return sess, nil
}
