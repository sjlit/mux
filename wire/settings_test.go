package wire_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sjlit/mux/wire"
)

func TestDefaultSettings(t *testing.T) {
	s := wire.DefaultSettings()
	if err := s.Validate(); err != nil {
		t.Fatalf("DefaultSettings is invalid: %v", err)
	}
	if s.Version != wire.Version {
		t.Fatalf("Version = %d, want %d", s.Version, wire.Version)
	}
	if s.MaxFrameSize != wire.DefaultMaxFrameSize {
		t.Fatalf("MaxFrameSize = %d, want %d", s.MaxFrameSize, wire.DefaultMaxFrameSize)
	}
	if s.InitialStreamWindow != wire.DefaultInitialStreamWindow {
		t.Fatalf("InitialStreamWindow = %d, want %d", s.InitialStreamWindow, wire.DefaultInitialStreamWindow)
	}
	if s.InitialSessionWindow != wire.DefaultInitialSessionWindow {
		t.Fatalf("InitialSessionWindow = %d, want %d", s.InitialSessionWindow, wire.DefaultInitialSessionWindow)
	}
	if s.MaxConcurrentStreams != wire.UnlimitedStreams {
		t.Fatalf("MaxConcurrentStreams = %d, want %d", s.MaxConcurrentStreams, wire.UnlimitedStreams)
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	tests := []wire.Settings{
		wire.DefaultSettings(),
		{
			Version:              wire.Version,
			MaxFrameSize:         65535,
			InitialStreamWindow:  wire.MaxWindow,
			InitialSessionWindow: 1,
			KeepAliveIntervalMS:  0,
			KeepAliveTimeoutMS:   0,
			MaxConcurrentStreams: 0,
		},
		{
			Version:              wire.Version,
			MaxFrameSize:         1,
			InitialStreamWindow:  1,
			InitialSessionWindow: wire.MaxWindow,
			KeepAliveIntervalMS:  1000,
			KeepAliveTimeoutMS:   1000,
			MaxConcurrentStreams: 12345,
		},
	}

	for _, want := range tests {
		payload, err := wire.AppendSettings(nil, want)
		if err != nil {
			t.Fatalf("AppendSettings(%+v): %v", want, err)
		}
		if len(payload) != wire.SettingsPayloadSize {
			t.Fatalf("payload len = %d, want %d", len(payload), wire.SettingsPayloadSize)
		}

		got, err := wire.ParseSettings(payload)
		if err != nil {
			t.Fatalf("ParseSettings(%+v): %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestAppendSettingsAppends(t *testing.T) {
	prefix := []byte{0xaa, 0xbb}
	got, err := wire.AppendSettings(prefix, wire.DefaultSettings())
	if err != nil {
		t.Fatalf("AppendSettings: %v", err)
	}
	if len(got) != len(prefix)+wire.SettingsPayloadSize {
		t.Fatalf("len = %d, want %d", len(got), len(prefix)+wire.SettingsPayloadSize)
	}
	if !bytes.Equal(got[:len(prefix)], prefix) {
		t.Fatalf("prefix = %x, want %x", got[:len(prefix)], prefix)
	}
}

func TestSettingsValidate(t *testing.T) {
	base := wire.DefaultSettings()

	tests := []struct {
		name   string
		mutate func(*wire.Settings)
		want   error
	}{
		{"bad-version", func(s *wire.Settings) { s.Version = 2 }, wire.ErrBadSettings},
		{"bad-flags", func(s *wire.Settings) { s.Flags = 1 }, wire.ErrBadSettings},
		{"max-frame-zero", func(s *wire.Settings) { s.MaxFrameSize = 0 }, wire.ErrBadSettings},
		{"max-frame-too-big", func(s *wire.Settings) { s.MaxFrameSize = wire.MaxPayloadSize + 1 }, wire.ErrBadSettings},
		{"stream-window-zero", func(s *wire.Settings) { s.InitialStreamWindow = 0 }, wire.ErrBadWindow},
		{"stream-window-too-big", func(s *wire.Settings) { s.InitialStreamWindow = wire.MaxWindow + 1 }, wire.ErrBadWindow},
		{"session-window-zero", func(s *wire.Settings) { s.InitialSessionWindow = 0 }, wire.ErrBadWindow},
		{"session-window-too-big", func(s *wire.Settings) { s.InitialSessionWindow = wire.MaxWindow + 1 }, wire.ErrBadWindow},
		{"keepalive-timeout-short", func(s *wire.Settings) {
			s.KeepAliveIntervalMS = 10
			s.KeepAliveTimeoutMS = 9
		}, wire.ErrBadKeepAlive},
		{"keepalive-timeout-zero", func(s *wire.Settings) {
			s.KeepAliveIntervalMS = 10
			s.KeepAliveTimeoutMS = 0
		}, wire.ErrBadKeepAlive},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if err := s.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("Validate error = %v, want %v", err, tc.want)
			}
			if _, err := wire.AppendSettings(nil, s); !errors.Is(err, tc.want) {
				t.Fatalf("AppendSettings error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSettingsValidEdgeCases(t *testing.T) {
	base := wire.DefaultSettings()

	tests := []struct {
		name   string
		mutate func(*wire.Settings)
	}{
		{"keepalive-disabled", func(s *wire.Settings) {
			s.KeepAliveIntervalMS = 0
			s.KeepAliveTimeoutMS = 0
		}},
		{"keepalive-disabled-nonzero-timeout", func(s *wire.Settings) {
			s.KeepAliveIntervalMS = 0
			s.KeepAliveTimeoutMS = 30000
		}},
		{"keepalive-equal", func(s *wire.Settings) {
			s.KeepAliveIntervalMS = 5000
			s.KeepAliveTimeoutMS = 5000
		}},
		{"min-frame-size", func(s *wire.Settings) { s.MaxFrameSize = 1 }},
		{"max-frame-size", func(s *wire.Settings) { s.MaxFrameSize = wire.MaxPayloadSize }},
		{"min-windows", func(s *wire.Settings) {
			s.InitialStreamWindow = 1
			s.InitialSessionWindow = 1
		}},
		{"max-windows", func(s *wire.Settings) {
			s.InitialStreamWindow = wire.MaxWindow
			s.InitialSessionWindow = wire.MaxWindow
		}},
		{"no-streams", func(s *wire.Settings) { s.MaxConcurrentStreams = 0 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if err := s.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestParseSettingsLength(t *testing.T) {
	for _, n := range []int{0, 1, wire.SettingsPayloadSize - 1, wire.SettingsPayloadSize + 1} {
		if _, err := wire.ParseSettings(make([]byte, n)); !errors.Is(err, wire.ErrBadLength) {
			t.Fatalf("len %d: error = %v, want ErrBadLength", n, err)
		}
	}
}

func TestParseSettingsValidation(t *testing.T) {
	payload, err := wire.AppendSettings(nil, wire.DefaultSettings())
	if err != nil {
		t.Fatalf("AppendSettings: %v", err)
	}

	// Version is the first uint16.
	bad := append([]byte(nil), payload...)
	bad[0] = 2
	if _, err := wire.ParseSettings(bad); !errors.Is(err, wire.ErrBadSettings) {
		t.Fatalf("bad version error = %v, want ErrBadSettings", err)
	}

	// Stream window is the u32 at offset 8.
	bad = append([]byte(nil), payload...)
	bad[8], bad[9], bad[10], bad[11] = 0, 0, 0, 0
	if _, err := wire.ParseSettings(bad); !errors.Is(err, wire.ErrBadWindow) {
		t.Fatalf("zero window error = %v, want ErrBadWindow", err)
	}
}

func TestNewSettingsFrame(t *testing.T) {
	f, err := wire.NewSettingsFrame(wire.DefaultSettings())
	if err != nil {
		t.Fatalf("NewSettingsFrame: %v", err)
	}
	if f.Type != wire.TypeSETTINGS {
		t.Fatalf("Type = %v, want SETTINGS", f.Type)
	}
	if f.StreamID != 0 {
		t.Fatalf("StreamID = %d, want 0", f.StreamID)
	}
	if len(f.Payload) != wire.SettingsPayloadSize {
		t.Fatalf("payload len = %d, want %d", len(f.Payload), wire.SettingsPayloadSize)
	}

	bad := wire.DefaultSettings()
	bad.Version = 2
	if _, err := wire.NewSettingsFrame(bad); !errors.Is(err, wire.ErrBadSettings) {
		t.Fatalf("NewSettingsFrame invalid error = %v, want ErrBadSettings", err)
	}
}
