// Package wire implements the mux v3 wire protocol codec.
//
// The frame format is specified in v3/PROTOCOL.md. The codec is stateless:
// DecodeFrame parses at most one frame from the front of a byte slice and
// reports how many bytes it consumed, so callers can feed it arbitrarily
// fragmented or coalesced transport reads.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Version is the mux v3 wire version.
	Version = 3

	// HeaderSize is the fixed frame header size in bytes.
	HeaderSize = 8

	// CreditSize is the size of the optional credit field carried by PSH
	// frames with FlagCredit set.
	CreditSize = 4

	// SettingsPayloadSize is the fixed SETTINGS payload size in bytes.
	SettingsPayloadSize = 28

	// MaxPayloadSize is the largest payload representable by the 16-bit
	// length field.
	MaxPayloadSize = 1<<16 - 1
)

// Type is the frame type carried in the high nibble of header byte 1.
type Type uint8

const (
	TypePSH      Type = 0x0 // stream data
	TypeUPD      Type = 0x1 // flow-control credit update
	TypeRST      Type = 0x2 // reset a stream
	TypePING     Type = 0x3 // liveness / RTT request
	TypePONG     Type = 0x4 // PING response
	TypeGOAWAY   Type = 0x5 // graceful session shutdown
	TypeSETTINGS Type = 0x6 // session parameters
)

func (t Type) String() string {
	switch t {
	case TypePSH:
		return "PSH"
	case TypeUPD:
		return "UPD"
	case TypeRST:
		return "RST"
	case TypePING:
		return "PING"
	case TypePONG:
		return "PONG"
	case TypeGOAWAY:
		return "GOAWAY"
	case TypeSETTINGS:
		return "SETTINGS"
	default:
		return fmt.Sprintf("Type(0x%x)", uint8(t))
	}
}

// Flags is the flag nibble carried in the low nibble of header byte 1.
type Flags uint8

const (
	// FlagSYN opens a stream. Valid only on PSH frames.
	FlagSYN Flags = 1 << 0
	// FlagFIN half-closes the sender's direction. Valid only on PSH frames.
	FlagFIN Flags = 1 << 1
	// FlagCredit adds a 4-byte absolute consumed counter after the header.
	// Valid only on PSH frames.
	FlagCredit Flags = 1 << 2
)

const flagMask = FlagSYN | FlagFIN | FlagCredit

var (
	// ErrShortFrame means the input does not yet contain a complete frame.
	ErrShortFrame = errors.New("wire: short frame")
	// ErrBadVersion means the frame version is not Version.
	ErrBadVersion = errors.New("wire: bad version")
	// ErrBadType means the frame type is unknown or reserved.
	ErrBadType = errors.New("wire: bad frame type")
	// ErrBadFlags means the flag nibble is invalid for the frame type.
	ErrBadFlags = errors.New("wire: bad frame flags")
	// ErrBadLength means the payload length is invalid for the frame type.
	ErrBadLength = errors.New("wire: bad frame length")
	// ErrFrameTooLarge means a PSH payload exceeds the local frame limit.
	ErrFrameTooLarge = errors.New("wire: frame too large")
	// ErrBadStreamID means the stream ID is invalid for the frame type.
	ErrBadStreamID = errors.New("wire: bad stream id")
	// ErrBadSettings means a SETTINGS value is out of range.
	ErrBadSettings = errors.New("wire: bad settings")
	// ErrBadWindow means a flow-control window is out of range.
	ErrBadWindow = errors.New("wire: bad flow-control window")
	// ErrBadKeepAlive means keepalive settings are inconsistent.
	ErrBadKeepAlive = errors.New("wire: bad keepalive settings")
)

// ErrorCode is a wire-level RST or GOAWAY reason code.
type ErrorCode uint32

const (
	CodeNoError           ErrorCode = 0x00
	CodeProtocolError     ErrorCode = 0x01
	CodeFlowControlError  ErrorCode = 0x02
	CodeStreamClosed      ErrorCode = 0x03
	CodeStreamIDExhausted ErrorCode = 0x04
	CodeFrameSizeError    ErrorCode = 0x05
	CodeInternalError     ErrorCode = 0x06
	CodeCancel            ErrorCode = 0x07
	CodeRefusedStream     ErrorCode = 0x08
	CodeEnhanceYourCalm   ErrorCode = 0x09
)

func (c ErrorCode) String() string {
	switch c {
	case CodeNoError:
		return "NO_ERROR"
	case CodeProtocolError:
		return "PROTOCOL_ERROR"
	case CodeFlowControlError:
		return "FLOW_CONTROL_ERROR"
	case CodeStreamClosed:
		return "STREAM_CLOSED"
	case CodeStreamIDExhausted:
		return "STREAM_ID_EXHAUSTED"
	case CodeFrameSizeError:
		return "FRAME_SIZE_ERROR"
	case CodeInternalError:
		return "INTERNAL_ERROR"
	case CodeCancel:
		return "CANCEL"
	case CodeRefusedStream:
		return "REFUSED_STREAM"
	case CodeEnhanceYourCalm:
		return "ENHANCE_YOUR_CALM"
	default:
		return fmt.Sprintf("ErrorCode(0x%02x)", uint32(c))
	}
}

// Frame is a decoded mux v3 frame.
//
// Payload is a sub-slice of the input passed to DecodeFrame; it is not copied.
// Credit is meaningful only when Flags&FlagCredit is set.
type Frame struct {
	Type     Type
	Flags    Flags
	StreamID uint32
	Credit   uint32
	Payload  []byte
}

// DecodeFrame parses one frame from the front of b.
//
// It returns the number of bytes consumed and, on success, a Frame whose
// Payload aliases b. A return value of ErrShortFrame means b does not contain a
// complete frame yet and the caller should read more data; n is 0 in that case.
//
// maxFrameSize bounds the PSH payload size and may be 0 to accept up to
// MaxPayloadSize. Control frames have fixed sizes and are not affected by it.
func DecodeFrame(b []byte, maxFrameSize uint32) (Frame, int, error) {
	f, total, err := decodeHeader(b, maxFrameSize)
	if err != nil {
		return Frame{}, 0, err
	}
	if len(b) < total {
		return Frame{}, 0, ErrShortFrame
	}

	creditSize := 0
	if f.Flags&FlagCredit != 0 {
		creditSize = CreditSize
		f.Credit = binary.LittleEndian.Uint32(b[HeaderSize : HeaderSize+CreditSize])
	}
	f.Payload = b[HeaderSize+creditSize : total]
	return f, total, nil
}

// DecodeHeader validates the fixed header at the front of b and returns the
// frame metadata plus the total size (header + credit + payload) of the
// complete frame. The returned Frame has a nil Payload and a zero Credit; the
// caller must read the remaining total-HeaderSize bytes to obtain them.
//
// This is useful for streaming readers that want to size the payload buffer
// before reading it. The header is fully validated, including type-specific
// lengths and stream-ID rules.
func DecodeHeader(b []byte, maxFrameSize uint32) (Frame, int, error) {
	return decodeHeader(b, maxFrameSize)
}

func decodeHeader(b []byte, maxFrameSize uint32) (Frame, int, error) {
	if len(b) < HeaderSize {
		return Frame{}, 0, ErrShortFrame
	}
	if b[0] != Version {
		return Frame{}, 0, fmt.Errorf("%w: got %d, want %d", ErrBadVersion, b[0], Version)
	}

	typ := Type(b[1] >> 4)
	flags := Flags(b[1] & 0x0f)
	if typ > TypeSETTINGS {
		return Frame{}, 0, fmt.Errorf("%w: %d", ErrBadType, uint8(typ))
	}
	if reserved := flags &^ flagMask; reserved != 0 {
		return Frame{}, 0, fmt.Errorf("%w: reserved bits 0x%x", ErrBadFlags, uint8(reserved))
	}
	if typ != TypePSH && flags != 0 {
		return Frame{}, 0, fmt.Errorf("%w: flags 0x%x on %s", ErrBadFlags, uint8(flags), typ)
	}

	length := int(binary.LittleEndian.Uint16(b[2:4]))
	streamID := binary.LittleEndian.Uint32(b[4:8])

	switch typ {
	case TypePSH:
		limit := maxFrameSize
		if limit == 0 || limit > MaxPayloadSize {
			limit = MaxPayloadSize
		}
		if uint32(length) > limit {
			return Frame{}, 0, fmt.Errorf("%w: payload %d exceeds limit %d", ErrFrameTooLarge, length, limit)
		}
		if streamID == 0 {
			return Frame{}, 0, fmt.Errorf("%w: PSH stream id 0 is reserved", ErrBadStreamID)
		}
	case TypeUPD:
		if length != 4 {
			return Frame{}, 0, fmt.Errorf("%w: UPD payload is %d, want 4", ErrBadLength, length)
		}
	case TypeRST:
		if length != 4 {
			return Frame{}, 0, fmt.Errorf("%w: RST payload is %d, want 4", ErrBadLength, length)
		}
		if streamID == 0 {
			return Frame{}, 0, fmt.Errorf("%w: RST stream id 0 is reserved", ErrBadStreamID)
		}
	case TypePING, TypePONG:
		if length != 0 {
			return Frame{}, 0, fmt.Errorf("%w: %s payload is %d, want 0", ErrBadLength, typ, length)
		}
	case TypeGOAWAY:
		if length != 4 {
			return Frame{}, 0, fmt.Errorf("%w: GOAWAY payload is %d, want 4", ErrBadLength, length)
		}
	case TypeSETTINGS:
		if length != SettingsPayloadSize {
			return Frame{}, 0, fmt.Errorf("%w: SETTINGS payload is %d, want %d", ErrBadLength, length, SettingsPayloadSize)
		}
		if streamID != 0 {
			return Frame{}, 0, fmt.Errorf("%w: SETTINGS stream id is %d, want 0", ErrBadStreamID, streamID)
		}
	}

	creditSize := 0
	if flags&FlagCredit != 0 {
		creditSize = CreditSize
	}
	total := HeaderSize + creditSize + length
	return Frame{
		Type:     typ,
		Flags:    flags,
		StreamID: streamID,
	}, total, nil
}

// AppendFrame validates f, encodes it, and appends the result to dst.
func AppendFrame(dst []byte, f Frame) ([]byte, error) {
	dst, err := AppendHeader(dst, f)
	if err != nil {
		return nil, err
	}
	dst = append(dst, f.Payload...)
	return dst, nil
}

// AppendHeader validates f and appends only its fixed header (and the optional
// credit field) to dst. The caller is responsible for appending exactly
// len(f.Payload) payload bytes after the returned slice.
//
// This is the zero-copy building block for batched writes: headers can be
// gathered in a small arena while payloads are written directly from the
// caller's buffers.
func AppendHeader(dst []byte, f Frame) ([]byte, error) {
	if err := validateFrame(f); err != nil {
		return nil, err
	}

	var hdr [HeaderSize]byte
	hdr[0] = Version
	hdr[1] = byte(f.Type)<<4 | byte(f.Flags)
	binary.LittleEndian.PutUint16(hdr[2:4], uint16(len(f.Payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], f.StreamID)
	dst = append(dst, hdr[:]...)

	if f.Flags&FlagCredit != 0 {
		var credit [CreditSize]byte
		binary.LittleEndian.PutUint32(credit[:], f.Credit)
		dst = append(dst, credit[:]...)
	}
	return dst, nil
}

// EncodedLen returns the number of bytes AppendFrame would emit for f.
func EncodedLen(f Frame) (int, error) {
	if err := validateFrame(f); err != nil {
		return 0, err
	}
	n := HeaderSize + len(f.Payload)
	if f.Flags&FlagCredit != 0 {
		n += CreditSize
	}
	return n, nil
}

func validateFrame(f Frame) error {
	if f.Type > TypeSETTINGS {
		return fmt.Errorf("%w: %d", ErrBadType, uint8(f.Type))
	}
	if reserved := f.Flags &^ flagMask; reserved != 0 {
		return fmt.Errorf("%w: reserved bits 0x%x", ErrBadFlags, uint8(reserved))
	}
	if f.Type != TypePSH && f.Flags != 0 {
		return fmt.Errorf("%w: flags 0x%x on %s", ErrBadFlags, uint8(f.Flags), f.Type)
	}
	if f.Flags&FlagCredit == 0 && f.Credit != 0 {
		return fmt.Errorf("%w: credit set without FlagCredit", ErrBadFlags)
	}
	if len(f.Payload) > MaxPayloadSize {
		return fmt.Errorf("%w: payload %d exceeds %d", ErrFrameTooLarge, len(f.Payload), MaxPayloadSize)
	}

	switch f.Type {
	case TypePSH:
		if f.StreamID == 0 {
			return fmt.Errorf("%w: PSH stream id 0 is reserved", ErrBadStreamID)
		}
	case TypeUPD:
		if len(f.Payload) != 4 {
			return fmt.Errorf("%w: UPD payload is %d, want 4", ErrBadLength, len(f.Payload))
		}
	case TypeRST:
		if f.StreamID == 0 {
			return fmt.Errorf("%w: RST stream id 0 is reserved", ErrBadStreamID)
		}
		if len(f.Payload) != 4 {
			return fmt.Errorf("%w: RST payload is %d, want 4", ErrBadLength, len(f.Payload))
		}
	case TypePING, TypePONG:
		if len(f.Payload) != 0 {
			return fmt.Errorf("%w: %s payload is %d, want 0", ErrBadLength, f.Type, len(f.Payload))
		}
	case TypeGOAWAY:
		if len(f.Payload) != 4 {
			return fmt.Errorf("%w: GOAWAY payload is %d, want 4", ErrBadLength, len(f.Payload))
		}
	case TypeSETTINGS:
		if f.StreamID != 0 {
			return fmt.Errorf("%w: SETTINGS stream id is %d, want 0", ErrBadStreamID, f.StreamID)
		}
		if len(f.Payload) != SettingsPayloadSize {
			return fmt.Errorf("%w: SETTINGS payload is %d, want %d", ErrBadLength, len(f.Payload), SettingsPayloadSize)
		}
	}
	return nil
}

// Consumed returns the absolute consumed counter carried by a UPD frame.
func (f Frame) Consumed() (uint32, error) {
	if f.Type != TypeUPD {
		return 0, fmt.Errorf("%w: %s has no consumed counter", ErrBadType, f.Type)
	}
	if len(f.Payload) != 4 {
		return 0, fmt.Errorf("%w: UPD payload is %d, want 4", ErrBadLength, len(f.Payload))
	}
	return binary.LittleEndian.Uint32(f.Payload), nil
}

// Code returns the error code carried by an RST or GOAWAY frame.
func (f Frame) Code() (ErrorCode, error) {
	if f.Type != TypeRST && f.Type != TypeGOAWAY {
		return 0, fmt.Errorf("%w: %s has no error code", ErrBadType, f.Type)
	}
	if len(f.Payload) != 4 {
		return 0, fmt.Errorf("%w: %s payload is %d, want 4", ErrBadLength, f.Type, len(f.Payload))
	}
	return ErrorCode(binary.LittleEndian.Uint32(f.Payload)), nil
}

// NewUPDFrame builds a UPD frame for a stream, or for the session when
// streamID is 0.
func NewUPDFrame(streamID, consumed uint32) Frame {
	var payload [4]byte
	binary.LittleEndian.PutUint32(payload[:], consumed)
	return Frame{Type: TypeUPD, StreamID: streamID, Payload: payload[:]}
}

// NewRSTFrame builds an RST frame.
func NewRSTFrame(streamID uint32, code ErrorCode) Frame {
	var payload [4]byte
	binary.LittleEndian.PutUint32(payload[:], uint32(code))
	return Frame{Type: TypeRST, StreamID: streamID, Payload: payload[:]}
}

// NewPINGFrame builds a PING frame. The nonce is carried in the StreamID field.
func NewPINGFrame(nonce uint32) Frame {
	return Frame{Type: TypePING, StreamID: nonce}
}

// NewPONGFrame builds a PONG frame echoing nonce.
func NewPONGFrame(nonce uint32) Frame {
	return Frame{Type: TypePONG, StreamID: nonce}
}

// NewGOAWAYFrame builds a GOAWAY frame. lastStreamID is the highest peer-
// initiated stream ID processed by the sender.
func NewGOAWAYFrame(lastStreamID uint32, code ErrorCode) Frame {
	var payload [4]byte
	binary.LittleEndian.PutUint32(payload[:], uint32(code))
	return Frame{Type: TypeGOAWAY, StreamID: lastStreamID, Payload: payload[:]}
}
