package wire

import (
	"encoding/binary"
	"fmt"
)

const (
	// DefaultMaxFrameSize is the default limit for a PSH payload.
	DefaultMaxFrameSize uint32 = 32768
	// DefaultInitialStreamWindow is the default per-stream flow-control window.
	DefaultInitialStreamWindow uint32 = 1 << 20
	// DefaultInitialSessionWindow is the default session-wide window.
	DefaultInitialSessionWindow uint32 = 8 << 20
	// DefaultKeepAliveIntervalMS is the default PING interval in milliseconds.
	DefaultKeepAliveIntervalMS uint32 = 10_000
	// DefaultKeepAliveTimeoutMS is the default keepalive timeout in milliseconds.
	DefaultKeepAliveTimeoutMS uint32 = 30_000
	// MaxWindow is the largest valid flow-control window.
	MaxWindow uint32 = 1<<31 - 1
	// UnlimitedStreams disables the concurrent-stream limit.
	UnlimitedStreams uint32 = ^uint32(0)
)

// Settings is the fixed-size SETTINGS payload.
type Settings struct {
	Version              uint16
	Flags                uint16
	MaxFrameSize         uint32
	InitialStreamWindow  uint32
	InitialSessionWindow uint32
	KeepAliveIntervalMS  uint32
	KeepAliveTimeoutMS   uint32
	MaxConcurrentStreams uint32
}

// DefaultSettings returns the v3 default settings.
func DefaultSettings() Settings {
	return Settings{
		Version:              Version,
		MaxFrameSize:         DefaultMaxFrameSize,
		InitialStreamWindow:  DefaultInitialStreamWindow,
		InitialSessionWindow: DefaultInitialSessionWindow,
		KeepAliveIntervalMS:  DefaultKeepAliveIntervalMS,
		KeepAliveTimeoutMS:   DefaultKeepAliveTimeoutMS,
		MaxConcurrentStreams: UnlimitedStreams,
	}
}

// Validate checks that s is within the ranges defined by the protocol.
func (s Settings) Validate() error {
	if s.Version != Version {
		return fmt.Errorf("%w: version %d, want %d", ErrBadSettings, s.Version, Version)
	}
	if s.Flags != 0 {
		return fmt.Errorf("%w: reserved flags 0x%x", ErrBadSettings, s.Flags)
	}
	if s.MaxFrameSize == 0 || s.MaxFrameSize > MaxPayloadSize {
		return fmt.Errorf("%w: max frame size %d outside 1..%d", ErrBadSettings, s.MaxFrameSize, MaxPayloadSize)
	}
	if s.InitialStreamWindow == 0 || s.InitialStreamWindow > MaxWindow {
		return fmt.Errorf("%w: stream window %d outside 1..%d", ErrBadWindow, s.InitialStreamWindow, MaxWindow)
	}
	if s.InitialSessionWindow == 0 || s.InitialSessionWindow > MaxWindow {
		return fmt.Errorf("%w: session window %d outside 1..%d", ErrBadWindow, s.InitialSessionWindow, MaxWindow)
	}
	if s.KeepAliveIntervalMS > 0 && s.KeepAliveTimeoutMS < s.KeepAliveIntervalMS {
		return fmt.Errorf("%w: timeout %d ms < interval %d ms", ErrBadKeepAlive, s.KeepAliveTimeoutMS, s.KeepAliveIntervalMS)
	}
	return nil
}

// AppendSettings validates s and appends its 28-byte encoding to dst.
func AppendSettings(dst []byte, s Settings) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	var buf [SettingsPayloadSize]byte
	binary.LittleEndian.PutUint16(buf[0:2], s.Version)
	binary.LittleEndian.PutUint16(buf[2:4], s.Flags)
	binary.LittleEndian.PutUint32(buf[4:8], s.MaxFrameSize)
	binary.LittleEndian.PutUint32(buf[8:12], s.InitialStreamWindow)
	binary.LittleEndian.PutUint32(buf[12:16], s.InitialSessionWindow)
	binary.LittleEndian.PutUint32(buf[16:20], s.KeepAliveIntervalMS)
	binary.LittleEndian.PutUint32(buf[20:24], s.KeepAliveTimeoutMS)
	binary.LittleEndian.PutUint32(buf[24:28], s.MaxConcurrentStreams)
	return append(dst, buf[:]...), nil
}

// ParseSettings decodes and validates a 28-byte SETTINGS payload.
func ParseSettings(b []byte) (Settings, error) {
	if len(b) != SettingsPayloadSize {
		return Settings{}, fmt.Errorf("%w: SETTINGS payload is %d, want %d", ErrBadLength, len(b), SettingsPayloadSize)
	}

	s := Settings{
		Version:              binary.LittleEndian.Uint16(b[0:2]),
		Flags:                binary.LittleEndian.Uint16(b[2:4]),
		MaxFrameSize:         binary.LittleEndian.Uint32(b[4:8]),
		InitialStreamWindow:  binary.LittleEndian.Uint32(b[8:12]),
		InitialSessionWindow: binary.LittleEndian.Uint32(b[12:16]),
		KeepAliveIntervalMS:  binary.LittleEndian.Uint32(b[16:20]),
		KeepAliveTimeoutMS:   binary.LittleEndian.Uint32(b[20:24]),
		MaxConcurrentStreams: binary.LittleEndian.Uint32(b[24:28]),
	}
	if err := s.Validate(); err != nil {
		return Settings{}, err
	}
	return s, nil
}

// NewSettingsFrame builds a SETTINGS frame carrying s.
func NewSettingsFrame(s Settings) (Frame, error) {
	payload, err := AppendSettings(nil, s)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Type: TypeSETTINGS, Payload: payload}, nil
}
