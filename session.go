package mux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sjlit/mux/wire"
)

// ctrlMsg is a control frame queued for the single writer.
type ctrlMsg struct {
	frame wire.Frame
	done  chan struct{} // closed after the frame is written or dropped
}

const (
	// ctrlQueueLimit bounds queued control frames. A peer that floods PING,
	// RST, or UPD frames must not be able to grow the queue without bound.
	ctrlQueueLimit = 4096
	// ctrlPongHighWater starts dropping PONGs before the hard limit so a PING
	// flood cannot crowd out flow-control and shutdown frames.
	ctrlPongHighWater = ctrlQueueLimit / 2
	// unknownFrameOverhead charges per-frame bookkeeping to the unknown-stream
	// budget. Without it, zero-length or one-byte frames cost nothing.
	unknownFrameOverhead = 256
	// unknownBudgetRefillPeriod is how long an idle session takes to refill a
	// completely exhausted unknown-stream budget. Refilling is lazy and only
	// happens when another unknown frame is charged.
	unknownBudgetRefillPeriod = 60 * time.Second
	// maxAutoUnknownBudget caps the budget derived from the session window so
	// an unusually large window cannot create a multi-gigabyte allowance.
	maxAutoUnknownBudget = 256 << 20
	// goAwayDrainTimeout bounds the best-effort GOAWAY write before a fatal
	// protocol error closes the transport.
	goAwayDrainTimeout = 100 * time.Millisecond
)

// Session multiplexes streams over one reliable, ordered byte stream.
type Session struct {
	conn   io.ReadWriteCloser
	config Config
	client bool

	streams   *streamTable
	chAccepts chan *Stream

	stateNotify  *notifier
	creditNotify *notifier

	die           chan struct{}
	closeOnce     sync.Once
	closeErr      error
	connCloseOnce sync.Once
	loops         sync.WaitGroup

	nextStreamID uint32
	nextIDMu     sync.Mutex
	goAway       atomic.Bool
	goAwaySent   atomic.Bool

	highestPeerID       uint32 // highest peer id seen, including refused streams
	lastProcessedPeerID uint32 // highest peer id queued for Accept (peerIDMu)
	peerGoAwayCode      wire.ErrorCode
	peerGoAwayLastID    uint32
	peerGoAwaySet       bool
	peerIDMu            sync.Mutex
	localOpen           atomic.Int32
	remoteOpen          atomic.Int32

	settingsMu      sync.Mutex
	peerSettingsSet atomic.Bool
	settingsSent    atomic.Bool
	peerMaxFrame    atomic.Uint32

	peerStreamWindow  atomic.Uint32
	peerSessionWindow atomic.Uint32
	peerMaxStreams    atomic.Uint32

	recvMaxFrame        uint32
	recvStreamWindow    uint32
	recvSessionWindow   uint32
	sessionRecvBytes    atomic.Uint32
	sessionConsumed     atomic.Uint32
	sessionUpdMu        sync.Mutex
	lastSessionSent     atomic.Uint32
	sessionNumWritten   atomic.Uint32
	sessionReserved     atomic.Uint32
	peerSessionConsumed atomic.Uint32

	sendMu sync.Mutex
	active []*Stream
	rr     int

	// connMu serializes the writer goroutine with the direct-write fast path.
	connMu sync.Mutex

	// Scratch space for direct writes. directMu keeps the header arena and
	// iovec slices valid while the caller is writing.
	directMu    sync.Mutex
	directHdr   [8 * wire.HeaderSize]byte
	directVec   [16][]byte
	directInfos [8]frameInfo

	ctrlMu     sync.Mutex
	ctrlQ      []ctrlMsg
	ctrlHead   int
	pendingRST map[uint32]struct{}
	wake       chan struct{}

	lastRecv       atomic.Int64
	pingNonce      atomic.Uint32
	pingSentAt     atomic.Int64
	lastRTT        atomic.Int64
	pendingReadErr error // recvLoop only: drain buffered frames before failing

	bufferedFrames       atomic.Int64 // queued PSH payloads across all streams
	unknownBudget        int64        // recvLoop only
	unknownBudgetMax     int64
	unknownBudgetUpdated time.Time // recvLoop only

	blockPool *blockPools

	pool *bufferPool
}

func newSession(config *Config, conn io.ReadWriteCloser, client bool) *Session {
	cfg := *config
	s := &Session{
		conn:              conn,
		config:            cfg,
		client:            client,
		streams:           newStreamTable(),
		chAccepts:         make(chan *Stream, cfg.AcceptBacklog),
		stateNotify:       newNotifier(),
		creditNotify:      newNotifier(),
		die:               make(chan struct{}),
		wake:              make(chan struct{}, 1),
		pool:              newBufferPool(),
		recvMaxFrame:      uint32(cfg.MaxFrameSize),
		recvStreamWindow:  uint32(cfg.InitialStreamWindow),
		recvSessionWindow: uint32(cfg.InitialSessionWindow),
	}
	s.peerMaxFrame.Store(wire.DefaultMaxFrameSize)
	s.peerStreamWindow.Store(wire.DefaultInitialStreamWindow)
	s.peerSessionWindow.Store(wire.DefaultInitialSessionWindow)
	s.peerMaxStreams.Store(wire.UnlimitedStreams)
	s.lastRecv.Store(time.Now().UnixNano())

	if client {
		s.nextStreamID = 1
	} else {
		s.nextStreamID = 2
	}

	s.unknownBudgetMax = int64(cfg.MaxUnknownStreamDiscard)
	if s.unknownBudgetMax <= 0 {
		s.unknownBudgetMax = int64(cfg.InitialSessionWindow) * 8
		if s.unknownBudgetMax < 1<<20 {
			s.unknownBudgetMax = 1 << 20
		}
		if s.unknownBudgetMax > maxAutoUnknownBudget {
			s.unknownBudgetMax = maxAutoUnknownBudget
		}
	}
	s.unknownBudget = s.unknownBudgetMax
	s.unknownBudgetUpdated = time.Now()

	settings := wire.Settings{
		Version:              uint16(wire.Version),
		MaxFrameSize:         uint32(cfg.MaxFrameSize),
		InitialStreamWindow:  uint32(cfg.InitialStreamWindow),
		InitialSessionWindow: uint32(cfg.InitialSessionWindow),
		KeepAliveIntervalMS:  uint32(cfg.KeepAliveInterval.Milliseconds()),
		KeepAliveTimeoutMS:   uint32(cfg.KeepAliveTimeout.Milliseconds()),
		MaxConcurrentStreams: cfg.MaxConcurrentStreams,
	}
	if frame, err := wire.NewSettingsFrame(settings); err != nil {
		s.fail(err)
		return s
	} else {
		s.enqueueCtrl(frame)
	}

	s.loops.Add(2)
	if cfg.ReadBatchSize > 0 {
		size := cfg.ReadBatchSize
		if min := cfg.MaxFrameSize + wire.HeaderSize + wire.CreditSize; size < min {
			size = min
		}
		s.blockPool = newBlockPools(8<<10, size)
		go s.recvLoopBatched()
	} else {
		go s.recvLoop()
	}
	go s.sendLoop()
	if !cfg.KeepAliveDisabled {
		s.loops.Add(1)
		go s.keepaliveLoop()
	}
	return s
}

func (s *Session) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Session) enqueueCtrl(f wire.Frame) {
	s.enqueueCtrlDone(f, nil)
}

func (s *Session) enqueueCtrlDone(f wire.Frame, done chan struct{}) {
	if len(f.Payload) != 0 {
		f.Payload = append([]byte(nil), f.Payload...)
	}

	var overflow error
	s.ctrlMu.Lock()
	if s.IsClosed() {
		s.ctrlMu.Unlock()
		if done != nil {
			close(done)
		}
		return
	}
	queued := len(s.ctrlQ) - s.ctrlHead
	if f.Type == wire.TypeRST {
		if s.pendingRST == nil {
			s.pendingRST = make(map[uint32]struct{})
		}
		if _, dup := s.pendingRST[f.StreamID]; dup {
			s.ctrlMu.Unlock()
			if done != nil {
				close(done)
			}
			return
		}
		if queued >= ctrlQueueLimit {
			overflow = fmt.Errorf("%w: control queue overflow", ErrFlowControl)
		} else {
			s.pendingRST[f.StreamID] = struct{}{}
		}
	} else if f.Type == wire.TypePONG && queued >= ctrlPongHighWater {
		// PONGs are expendable under a PING flood; dropping them keeps
		// flow-control and shutdown frames bounded.
		s.ctrlMu.Unlock()
		if done != nil {
			close(done)
		}
		return
	} else if queued >= ctrlQueueLimit {
		overflow = fmt.Errorf("%w: control queue overflow", ErrFlowControl)
	}
	if overflow != nil {
		s.ctrlMu.Unlock()
		if done != nil {
			close(done)
		}
		s.fail(overflow)
		return
	}

	s.ctrlQ = append(s.ctrlQ, ctrlMsg{frame: f, done: done})
	s.ctrlMu.Unlock()
	s.signal()
}

func (s *Session) ctrlLen() int {
	s.ctrlMu.Lock()
	n := len(s.ctrlQ) - s.ctrlHead
	s.ctrlMu.Unlock()
	return n
}

func (s *Session) takeCtrl(dst []ctrlMsg, max int) []ctrlMsg {
	s.ctrlMu.Lock()
	if s.ctrlHead >= len(s.ctrlQ) {
		s.ctrlQ = s.ctrlQ[:0]
		s.ctrlHead = 0
		s.ctrlMu.Unlock()
		return dst[:0]
	}
	n := len(s.ctrlQ) - s.ctrlHead
	if max > n {
		max = n
	}
	dst = append(dst[:0], s.ctrlQ[s.ctrlHead:s.ctrlHead+max]...)
	for i := s.ctrlHead; i < s.ctrlHead+max; i++ {
		if s.ctrlQ[i].frame.Type == wire.TypeRST {
			delete(s.pendingRST, s.ctrlQ[i].frame.StreamID)
		}
		s.ctrlQ[i] = ctrlMsg{}
	}
	s.ctrlHead += max
	if s.ctrlHead == len(s.ctrlQ) {
		s.ctrlQ = s.ctrlQ[:0]
		s.ctrlHead = 0
	} else if s.ctrlHead >= 256 && s.ctrlHead*2 >= len(s.ctrlQ) {
		// Compact only when the consumed prefix is large enough; advancing a
		// head index keeps the common path O(1) instead of shifting the queue.
		n = copy(s.ctrlQ, s.ctrlQ[s.ctrlHead:])
		for i := n; i < len(s.ctrlQ); i++ {
			s.ctrlQ[i] = ctrlMsg{}
		}
		s.ctrlQ = s.ctrlQ[:n]
		s.ctrlHead = 0
	}
	s.ctrlMu.Unlock()
	return dst
}

// IsClosed reports whether the session has shut down.
func (s *Session) IsClosed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

// CloseChan is closed when the session shuts down.
func (s *Session) CloseChan() <-chan struct{} {
	return s.die
}

// RTT returns the most recently measured PING/PONG round-trip time, or zero
// when no RTT sample is available.
func (s *Session) RTT() time.Duration {
	if ns := s.lastRTT.Load(); ns > 0 {
		return time.Duration(ns)
	}
	return 0
}

// CloseError returns the reason the session closed, or nil while it is open.
func (s *Session) CloseError() error {
	return s.closeError()
}

// PeerGoAway reports the error code and effective last processed stream ID
// from GOAWAY frames received from the peer.
func (s *Session) PeerGoAway() (wire.ErrorCode, uint32, bool) {
	s.peerIDMu.Lock()
	defer s.peerIDMu.Unlock()
	return s.peerGoAwayCode, s.peerGoAwayLastID, s.peerGoAwaySet
}

func (s *Session) closeError() error {
	select {
	case <-s.die:
		if s.closeErr != nil {
			return s.closeErr
		}
		return ErrSessionClosed
	default:
		return nil
	}
}

// NumStreams returns the number of streams currently tracked by the session.
func (s *Session) NumStreams() int {
	return s.streams.len()
}

// LocalAddr returns the local address of the underlying connection, if any.
func (s *Session) LocalAddr() net.Addr {
	if conn, ok := s.conn.(interface{ LocalAddr() net.Addr }); ok {
		return conn.LocalAddr()
	}
	return nil
}

// RemoteAddr returns the remote address of the underlying connection, if any.
func (s *Session) RemoteAddr() net.Addr {
	if conn, ok := s.conn.(interface{ RemoteAddr() net.Addr }); ok {
		return conn.RemoteAddr()
	}
	return nil
}

// OpenStream opens a new stream.
func (s *Session) OpenStream() (*Stream, error) {
	return s.OpenStreamContext(context.Background())
}

// OpenStreamContext opens a new stream, honoring ctx while waiting for a
// stream slot and for the SYN frame to be admitted.
func (s *Session) OpenStreamContext(ctx context.Context) (*Stream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(s.config.OpenTimeout)
	ctxDeadline := false
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
		ctxDeadline = true
	}
	timer := time.NewTimer(time.Until(deadline))
	defer stopTimer(timer)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			if ctxDeadline {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return nil, ErrTimeout
		}
		if s.IsClosed() {
			return nil, s.closeError()
		}
		if s.goAway.Load() {
			return nil, ErrGoAway
		}

		// Register for state changes before checking the slot limit. Without
		// this ordering, a stream close between the check and wait() would be
		// lost and OpenStream could sleep until its timeout.
		wake := s.stateNotify.wait()
		st, wait, err := s.newLocalStream()
		if err != nil {
			return nil, err
		}
		if wait {
			select {
			case <-wake:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.die:
				return nil, s.closeError()
			case <-timer.C:
				if ctxDeadline {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return nil, ErrTimeout
			}
		}
		return s.finishOpen(ctx, st, deadline, ctxDeadline)
	}
}

func (s *Session) newLocalStream() (*Stream, bool, error) {
	if s.IsClosed() {
		return nil, false, s.closeError()
	}
	if s.goAway.Load() {
		return nil, false, ErrGoAway
	}

	// Reserve a concurrent-stream slot before allocating the id. A plain
	// load-then-add lets two OpenStream calls both pass a peer limit of one.
	reserved := false
	if limit := s.peerMaxStreams.Load(); limit != wire.UnlimitedStreams {
		if limit == 0 {
			return nil, false, ErrRefusedStream
		}
		for {
			cur := s.localOpen.Load()
			if uint32(cur) >= limit {
				return nil, true, nil
			}
			if s.localOpen.CompareAndSwap(cur, cur+1) {
				reserved = true
				break
			}
		}
	}

	s.nextIDMu.Lock()
	if s.nextStreamID > math.MaxUint32-2 {
		s.goAway.Store(true)
		s.nextIDMu.Unlock()
		if reserved {
			s.localOpen.Add(-1)
		}
		if s.goAwaySent.CompareAndSwap(false, true) {
			s.enqueueCtrl(wire.NewGOAWAYFrame(s.lastPeerStreamID(), wire.CodeStreamIDExhausted))
		}
		s.stateNotify.notify()
		return nil, false, ErrStreamIDExhausted
	}
	id := s.nextStreamID
	s.nextStreamID += 2
	s.nextIDMu.Unlock()

	st := newStream(s, id, true, s.peerStreamWindow.Load())
	if !s.streams.add(id, st) {
		if reserved {
			s.localOpen.Add(-1)
			s.stateNotify.notify()
		}
		return nil, false, fmt.Errorf("%w: duplicate local stream id %d", ErrProtocol, id)
	}
	if !reserved {
		s.localOpen.Add(1)
	}
	s.stateNotify.notify()

	if s.IsClosed() {
		s.removeStream(st)
		return nil, false, s.closeError()
	}
	// Close/GOAWAY may have won the race after the initial check. Remove the
	// just-created stream instead of returning one the peer will not process.
	if s.goAway.Load() {
		s.removeStream(st)
		return nil, false, ErrGoAway
	}
	return st, false, nil
}

func (s *Session) finishOpen(ctx context.Context, st *Stream, deadline time.Time, ctxDeadline bool) (*Stream, error) {
	b := newWriteBatch(st, nil, wire.FlagSYN, getDoneChan())
	s.enqueueBatch(st, b)

	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	defer stopTimer(timer)

	select {
	case <-b.done:
		err := b.err
		b.release()
		if err != nil {
			s.removeStream(st)
			return nil, err
		}
		if st.aborted.Load() {
			s.removeStream(st)
			return nil, st.getErr()
		}
		return st, nil
	case <-ctx.Done():
		b.canceled.Store(true)
		s.signal()
		st.Reset(wire.CodeCancel)
		return nil, ctx.Err()
	case <-timer.C:
		b.canceled.Store(true)
		s.signal()
		st.Reset(wire.CodeCancel)
		if ctxDeadline {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, context.DeadlineExceeded
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, ErrTimeout
	case <-s.die:
		b.canceled.Store(true)
		s.signal()
		s.removeStream(st)
		return nil, s.closeError()
	}
}

// AcceptStream accepts the next peer-initiated stream.
func (s *Session) AcceptStream() (*Stream, error) {
	return s.AcceptStreamContext(context.Background())
}

// AcceptStreamContext accepts the next peer-initiated stream, honoring ctx.
func (s *Session) AcceptStreamContext(ctx context.Context) (*Stream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case st := <-s.chAccepts:
			if st == nil {
				continue
			}
			st.acceptMu.Lock()
			if st.resetBeforeAccept.Load() {
				st.acceptMu.Unlock()
				continue
			}
			st.accepted.Store(true)
			st.acceptMu.Unlock()
			if s.IsClosed() {
				return nil, s.closeError()
			}
			return st, nil
		case <-s.die:
			return nil, s.closeError()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Session) lastPeerStreamID() uint32 {
	s.peerIDMu.Lock()
	id := s.lastProcessedPeerID
	s.peerIDMu.Unlock()
	return id
}

func (s *Session) newIncomingStream(f wire.Frame) (*Stream, error) {
	if s.IsClosed() {
		return nil, s.closeError()
	}
	if s.goAway.Load() {
		return nil, ErrRefusedStream
	}
	id := f.StreamID
	if s.client {
		if id%2 != 0 {
			return nil, fmt.Errorf("%w: peer stream id %d has wrong parity", ErrProtocol, id)
		}
	} else if id%2 != 1 {
		return nil, fmt.Errorf("%w: peer stream id %d has wrong parity", ErrProtocol, id)
	}

	s.peerIDMu.Lock()
	if id <= s.highestPeerID {
		s.peerIDMu.Unlock()
		return nil, fmt.Errorf("%w: peer stream id %d is not increasing", ErrProtocol, id)
	}
	s.highestPeerID = id
	s.peerIDMu.Unlock()

	if limit := s.config.MaxConcurrentStreams; limit != wire.UnlimitedStreams {
		if uint32(s.remoteOpen.Load()) >= limit {
			return nil, ErrRefusedStream
		}
	}

	st := newStream(s, id, false, s.peerStreamWindow.Load())
	if !s.streams.add(id, st) {
		return nil, fmt.Errorf("%w: duplicate stream id %d", ErrProtocol, id)
	}
	s.remoteOpen.Add(1)
	s.stateNotify.notify()

	select {
	case s.chAccepts <- st:
		if s.IsClosed() {
			st.acceptMu.Lock()
			st.resetBeforeAccept.Store(true)
			st.acceptMu.Unlock()
			s.removeStream(st)
			return nil, s.closeError()
		}
		s.peerIDMu.Lock()
		if id > s.lastProcessedPeerID {
			s.lastProcessedPeerID = id
		}
		s.peerIDMu.Unlock()
		return st, nil
	default:
		s.streams.del(id)
		s.remoteOpen.Add(-1)
		s.stateNotify.notify()
		return nil, ErrRefusedStream
	}
}

func (s *Session) handlePSH(f wire.Frame, owner frameOwner) {
	st := s.streams.get(f.StreamID)
	if st == nil {
		if f.Flags&wire.FlagSYN == 0 {
			// A zero-length, flag-free PSH is a protocol no-op. Ignoring it
			// here also prevents an unknown-stream probe from generating an
			// unbounded stream of RST frames.
			if f.Flags == 0 && len(f.Payload) == 0 {
				owner.release()
				return
			}
			s.discardPSH(f, owner)
			return
		}
		var err error
		st, err = s.newIncomingStream(f)
		if err != nil {
			if errors.Is(err, ErrRefusedStream) {
				s.discardPayload(f, owner)
				s.enqueueCtrl(wire.NewRSTFrame(f.StreamID, wire.CodeRefusedStream))
				return
			}
			owner.release()
			s.fail(err)
			return
		}
	} else if f.Flags&wire.FlagSYN != 0 {
		owner.release()
		s.fail(fmt.Errorf("%w: duplicate SYN on stream %d", ErrProtocol, st.id))
		return
	}

	if f.Flags&wire.FlagCredit != 0 {
		st.applyPeerCredit(f.Credit)
		if s.IsClosed() {
			owner.release()
			return
		}
	}
	st.handleData(f, owner)
}

func (s *Session) discardPSH(f wire.Frame, owner frameOwner) {
	s.discardPayload(f, owner)
	if !s.IsClosed() {
		s.enqueueCtrl(wire.NewRSTFrame(f.StreamID, wire.CodeStreamClosed))
	}
}

// refillUnknownBudget lazily restores the unknown-stream discard budget. A
// fully exhausted budget recovers over unknownBudgetRefillPeriod, which keeps
// ordinary close races from eventually killing a long-lived session while
// still bounding sustained garbage traffic.
func (s *Session) refillUnknownBudget(now time.Time) {
	if s.unknownBudgetMax <= 0 || s.unknownBudget >= s.unknownBudgetMax {
		s.unknownBudgetUpdated = now
		return
	}
	elapsed := now.Sub(s.unknownBudgetUpdated)
	if elapsed <= 0 {
		return
	}
	if elapsed >= unknownBudgetRefillPeriod {
		s.unknownBudget = s.unknownBudgetMax
		s.unknownBudgetUpdated = now
		return
	}

	rate := s.unknownBudgetMax / int64(unknownBudgetRefillPeriod/time.Second)
	if rate < 1 {
		rate = 1
	}
	// Split the duration to keep the multiply from overflowing for
	// user-configured budgets that are close to MaxInt64.
	add := int64(elapsed/time.Second)*rate +
		int64(elapsed%time.Second)*rate/int64(time.Second)
	if add <= 0 {
		return
	}
	s.unknownBudget += add
	if s.unknownBudget > s.unknownBudgetMax {
		s.unknownBudget = s.unknownBudgetMax
	}
	s.unknownBudgetUpdated = now
}

// failWithGoAway makes one bounded attempt to tell the peer why the session
// is being torn down, then fails it. It must only be called from recvLoop or
// another goroutine that is not the writer.
func (s *Session) failWithGoAway(code wire.ErrorCode, err error) {
	if s.IsClosed() {
		s.fail(err)
		return
	}
	s.goAway.Store(true)
	s.goAwaySent.Store(true)
	s.stateNotify.notify()
	done := make(chan struct{})
	s.enqueueCtrlDone(wire.NewGOAWAYFrame(s.lastPeerStreamID(), code), done)
	timer := time.NewTimer(goAwayDrainTimeout)
	select {
	case <-done:
	case <-timer.C:
	case <-s.die:
	}
	stopTimer(timer)
	s.fail(err)
}

func (s *Session) discardPayload(f wire.Frame, owner frameOwner) {
	n := len(f.Payload)
	owner.release()
	// Charge per-frame bookkeeping as well as payload bytes. Otherwise a peer
	// can send an unlimited number of zero-length or one-byte frames without
	// ever exhausting the budget.
	s.refillUnknownBudget(time.Now())
	s.unknownBudget -= int64(n) + unknownFrameOverhead
	if n > 0 {
		s.sessionRecvBytes.Add(uint32(n))
		s.sessionConsumed.Add(uint32(n))
		s.publishSessionCredit()
	}
	if s.unknownBudget < 0 {
		s.failWithGoAway(wire.CodeEnhanceYourCalm,
			fmt.Errorf("%w: unknown-stream discard budget exhausted", ErrProtocol))
	}
}

func (s *Session) handleControl(f wire.Frame) error {
	switch f.Type {
	case wire.TypeSETTINGS:
		return s.handleSettings(f)
	case wire.TypeUPD:
		consumed, err := f.Consumed()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProtocol, err)
		}
		if f.StreamID == 0 {
			s.applySessionCredit(consumed)
		} else if st := s.streams.get(f.StreamID); st != nil {
			st.applyPeerCredit(consumed)
		}
	case wire.TypeRST:
		code, err := f.Code()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProtocol, err)
		}
		if st := s.streams.get(f.StreamID); st != nil {
			st.remoteReset(code)
		}
	case wire.TypePING:
		s.enqueueCtrl(wire.NewPONGFrame(f.StreamID))
	case wire.TypePONG:
		if f.StreamID == s.pingNonce.Load() {
			if sent := s.pingSentAt.Load(); sent != 0 {
				s.lastRTT.Store(time.Now().UnixNano() - sent)
			}
		}
	case wire.TypeGOAWAY:
		code, err := f.Code()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProtocol, err)
		}
		last := f.StreamID
		s.peerIDMu.Lock()
		if !s.peerGoAwaySet {
			s.peerGoAwayCode = code
			s.peerGoAwayLastID = last
			s.peerGoAwaySet = true
		} else if last < s.peerGoAwayLastID {
			s.peerGoAwayLastID = last
		}
		effectiveLast := s.peerGoAwayLastID
		s.peerIDMu.Unlock()

		s.goAway.Store(true)
		s.stateNotify.notify()
		// Streams above last_stream_id were not processed by the peer. Fail
		// them now instead of waiting for an RST that may never arrive.
		for _, st := range s.streams.snapshot() {
			if st.local && st.id > effectiveLast {
				st.remoteReset(wire.CodeRefusedStream)
			}
		}
	default:
		return fmt.Errorf("%w: unexpected control frame %s", ErrProtocol, f.Type)
	}
	return nil
}

func (s *Session) handleSettings(f wire.Frame) error {
	settings, err := wire.ParseSettings(f.Payload)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProtocol, err)
	}

	s.settingsMu.Lock()
	if s.peerSettingsSet.Load() {
		s.settingsMu.Unlock()
		return fmt.Errorf("%w: duplicate SETTINGS", ErrProtocol)
	}
	baseStream := uint32(wire.DefaultInitialStreamWindow)
	if uint32(s.config.InitialStreamWindow) < baseStream {
		baseStream = uint32(s.config.InitialStreamWindow)
	}
	baseSession := uint32(wire.DefaultInitialSessionWindow)
	if uint32(s.config.InitialSessionWindow) < baseSession {
		baseSession = uint32(s.config.InitialSessionWindow)
	}
	if settings.InitialStreamWindow < baseStream ||
		settings.InitialSessionWindow < baseSession {
		s.settingsMu.Unlock()
		return fmt.Errorf("%w: SETTINGS must not shrink flow-control windows", ErrProtocol)
	}
	maxFrame := s.config.MaxFrameSize
	if int(settings.MaxFrameSize) < maxFrame {
		maxFrame = int(settings.MaxFrameSize)
	}
	s.peerMaxFrame.Store(uint32(maxFrame))
	s.peerStreamWindow.Store(settings.InitialStreamWindow)
	s.peerSessionWindow.Store(settings.InitialSessionWindow)
	s.peerMaxStreams.Store(settings.MaxConcurrentStreams)
	s.peerSettingsSet.Store(true)
	s.settingsMu.Unlock()

	for _, st := range s.streams.snapshot() {
		st.peerWindow.Store(settings.InitialStreamWindow)
	}
	s.creditNotify.notify()
	s.stateNotify.notify()
	return nil
}

func (s *Session) applySessionCredit(consumed uint32) {
	written := s.sessionNumWritten.Load()
	if itimediff(consumed, written) > 0 {
		s.fail(fmt.Errorf("%w: session consumed %d exceeds written %d", ErrFlowControl, consumed, written))
		return
	}
	if itimediff(consumed, s.peerSessionConsumed.Load()) <= 0 {
		return
	}
	s.peerSessionConsumed.Store(consumed)
	s.creditNotify.notify()
}

// publishSessionCredit combines the normal half-window update with the
// pressure-driven one. Discard paths use it because they may advance consumed
// while a Read is already blocked on another stream.
func (s *Session) publishSessionCredit() {
	if s.sessionWindowPressure() {
		s.sendSessionUPD(true)
		return
	}
	s.sendSessionUPD(false)
}

// sessionWindowPressure reports whether enough received-but-unconsumed data
// is queued that the peer may be blocked on the session window.
func (s *Session) sessionWindowPressure() bool {
	inflight := int32(s.sessionRecvBytes.Load() - s.sessionConsumed.Load())
	return inflight >= int32(s.recvSessionWindow/2)
}

// maybeSendSessionUPDOnPressure publishes pending consumed credit when incoming
// data pushes the session buffer past its half-window mark. This matters when
// a Read is already blocked on another stream and cannot itself trigger the
// update.
func (s *Session) maybeSendSessionUPDOnPressure() {
	consumed := s.sessionConsumed.Load()
	if itimediff(consumed, s.lastSessionSent.Load()) <= 0 {
		return
	}
	if !s.sessionWindowPressure() {
		return
	}
	s.sendSessionUPD(true)
}

// sendSessionUPD advertises the absolute session consumed counter. force
// bypasses the half-window threshold and is used before a Read blocks and
// when buffered data is discarded, so other streams cannot be left without
// session credit.
func (s *Session) sendSessionUPD(force bool) {
	consumed := s.sessionConsumed.Load()
	threshold := int32(s.recvSessionWindow / 2)

	s.sessionUpdMu.Lock()
	delta := itimediff(consumed, s.lastSessionSent.Load())
	if delta <= 0 || (!force && delta < threshold) {
		s.sessionUpdMu.Unlock()
		return
	}
	s.lastSessionSent.Store(consumed)
	s.sessionUpdMu.Unlock()
	s.enqueueCtrl(wire.NewUPDFrame(0, consumed))
}

func (s *Session) checkSessionWindow(n int) bool {
	inflight := int32(s.sessionRecvBytes.Load() - s.sessionConsumed.Load())
	if inflight < 0 {
		return false
	}
	return inflight+int32(n) <= int32(s.recvSessionWindow)
}

func (s *Session) sendFrameSize() int {
	n := int(s.peerMaxFrame.Load())
	if n > s.config.MaxFrameSize {
		n = s.config.MaxFrameSize
	}
	if n < 1 {
		n = 1
	}
	return n
}

func (s *Session) sessionAvailable() int {
	inflight := int32(s.sessionNumWritten.Load() + s.sessionReserved.Load() - s.peerSessionConsumed.Load())
	if inflight < 0 {
		return 0
	}
	avail := int32(s.peerSessionWindow.Load()) - inflight
	if avail <= 0 {
		return 0
	}
	return int(avail)
}

func (s *Session) removeStream(st *Stream) {
	if !st.removed.CompareAndSwap(false, true) {
		return
	}
	s.streams.del(st.id)
	if st.local {
		s.localOpen.Add(-1)
	} else {
		s.remoteOpen.Add(-1)
	}
	s.stateNotify.notify()
	s.creditNotify.notify()
}

func (s *Session) drainAccepts(err error) {
	for {
		select {
		case st := <-s.chAccepts:
			if st != nil {
				st.sessionClosed(err)
			}
		default:
			return
		}
	}
}

// rejectAccepts discards peer streams that were never handed to the
// application. It is used during graceful shutdown so Close does not wait for
// an accept loop that has already stopped.
func (s *Session) rejectAccepts(err error) {
	for {
		select {
		case st := <-s.chAccepts:
			if st == nil {
				continue
			}
			if !st.accepted.Load() && !s.IsClosed() {
				s.enqueueCtrl(wire.NewRSTFrame(st.id, wire.CodeRefusedStream))
			}
			st.sessionClosed(err)
		default:
			return
		}
	}
}

func (s *Session) fail(err error) {
	if err == nil {
		err = ErrSessionClosed
	}
	s.closeOnce.Do(func() {
		s.closeErr = wrapSessionError(err)
		close(s.die)
		s.connCloseOnce.Do(func() {
			_ = s.conn.Close()
		})
		for _, st := range s.streams.snapshot() {
			st.sessionClosed(err)
		}
		s.drainAccepts(err)
		s.signal()
		s.stateNotify.notify()
		s.creditNotify.notify()
	})
}

// failRead terminates the session after a transport read error, but lets
// already accepted streams drain payloads that were parsed before the error.
// This matters when a reader returns (n>0, io.EOF): the last complete frames
// must be visible to the application before it observes the error.
func (s *Session) failRead(err error) {
	if err == nil {
		err = ErrSessionClosed
	}
	s.closeOnce.Do(func() {
		s.closeErr = wrapSessionError(err)
		s.goAway.Store(true)
		// Mark streams before closing the session die. A concurrent Read can
		// wake on the session die and return the transport error immediately;
		// the stream must already know to drain its buffered payloads first.
		for _, st := range s.streams.snapshot() {
			if st.accepted.Load() {
				st.sessionReadError(err)
			} else {
				st.sessionClosed(err)
			}
		}
		close(s.die)
		s.connCloseOnce.Do(func() {
			_ = s.conn.Close()
		})
		s.drainAccepts(err)
		s.signal()
		s.stateNotify.notify()
		s.creditNotify.notify()
	})
}

// Close gracefully shuts the session down: it sends GOAWAY, rejects streams
// still waiting in the accept queue, waits up to CloseTimeout for accepted
// streams, then closes the underlying connection. It returns the context
// deadline error if the drain does not finish in time.
func (s *Session) Close() error {
	if s.IsClosed() {
		s.loops.Wait()
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.CloseTimeout)
	defer cancel()
	return s.CloseGraceful(ctx)
}

// CloseGraceful is Close with a caller-provided drain deadline. It returns
// ctx.Err() when the deadline/cancellation prevents a clean drain.
func (s *Session) CloseGraceful(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.IsClosed() {
		s.loops.Wait()
		return nil
	}
	s.goAway.Store(true)
	s.stateNotify.notify()
	// Unaccepted streams are not owned by the application; release them
	// before blocking on GOAWAY or on the accepted-stream drain.
	s.rejectAccepts(ErrGoAway)

	timedOut := false
	if !s.goAwaySent.Swap(true) {
		done := make(chan struct{})
		s.enqueueCtrlDone(wire.NewGOAWAYFrame(s.lastPeerStreamID(), wire.CodeNoError), done)
		select {
		case <-done:
		case <-ctx.Done():
			timedOut = true
		case <-s.die:
		}
	}

drain:
	for {
		// Streams that were never accepted are not owned by the application.
		// Reject them so Close only waits for accepted streams.
		s.rejectAccepts(ErrGoAway)
		if s.streams.len() == 0 {
			break
		}
		wake := s.stateNotify.wait()
		s.rejectAccepts(ErrGoAway)
		if s.streams.len() == 0 {
			break
		}
		select {
		case <-wake:
		case <-ctx.Done():
			timedOut = true
			break drain
		case <-s.die:
			break drain
		}
	}

	s.fail(ErrSessionClosed)
	s.loops.Wait()
	if timedOut {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrTimeout
	}
	return nil
}

// Abort immediately closes the session and waits for its goroutines to exit.
func (s *Session) Abort() error {
	s.fail(ErrSessionClosed)
	s.loops.Wait()
	return nil
}

// recvScratchSize is the read-ahead buffer for the per-frame receive path.
// One read can capture a whole small frame (header + payload); large frames
// are completed with a direct read into their pooled payload buffer.
const recvScratchSize = 4 << 10

func (s *Session) recvLoop() {
	defer s.loops.Done()

	var scratch [recvScratchSize]byte
	n, off := 0, 0

	for {
		if s.IsClosed() {
			return
		}

		// Drain every complete frame already buffered before reading again.
		// Reading first would deadlock at a flow-control window boundary.
		for n-off >= wire.HeaderSize {
			hf, total, err := wire.DecodeHeader(scratch[off:n], s.recvMaxFrame)
			if err != nil {
				s.fail(fmt.Errorf("%w: %v", ErrProtocol, err))
				return
			}
			if !s.peerSettingsSet.Load() && hf.Type != wire.TypeSETTINGS {
				s.fail(fmt.Errorf("%w: first frame is %s, want SETTINGS", ErrProtocol, hf.Type))
				return
			}

			if total > len(scratch) {
				// The frame does not fit in the scratch buffer. Copy the
				// buffered prefix into its payload buffer and read the rest
				// directly, so a large frame costs two reads, not many.
				bodyLen := total - wire.HeaderSize
				creditSize := 0
				if hf.Flags&wire.FlagCredit != 0 {
					creditSize = wire.CreditSize
					bodyLen -= creditSize
				}
				have := n - off - wire.HeaderSize
				body := s.pool.get(bodyLen)

				var credit uint32
				if creditSize != 0 {
					if have >= creditSize {
						credit = binary.LittleEndian.Uint32(scratch[off+wire.HeaderSize : off+wire.HeaderSize+creditSize])
					} else {
						var c [wire.CreditSize]byte
						copy(c[:], scratch[off+wire.HeaderSize:n])
						if _, err := io.ReadFull(s.conn, c[have:creditSize]); err != nil {
							s.pool.put(body)
							s.failRead(err)
							return
						}
						credit = binary.LittleEndian.Uint32(c[:])
						have = 0
					}
				}
				payloadHave := have - creditSize
				if payloadHave < 0 {
					payloadHave = 0
				}
				if payloadHave > 0 {
					copy(*body, scratch[off+wire.HeaderSize+creditSize:n])
				}
				if _, err := io.ReadFull(s.conn, (*body)[payloadHave:]); err != nil {
					s.pool.put(body)
					s.failRead(err)
					return
				}
				n, off = 0, 0
				s.lastRecv.Store(time.Now().UnixNano())

				s.handlePSH(wire.Frame{
					Type:     hf.Type,
					Flags:    hf.Flags,
					StreamID: hf.StreamID,
					Credit:   credit,
					Payload:  *body,
				}, frameOwner{buf: body, pool: s.pool})
				if s.IsClosed() {
					return
				}
				continue
			}

			if n-off < total {
				break
			}

			frameStart := off
			frameEnd := off + total
			if hf.Type == wire.TypePSH {
				payloadStart := frameStart + wire.HeaderSize
				var credit uint32
				if hf.Flags&wire.FlagCredit != 0 {
					credit = binary.LittleEndian.Uint32(scratch[payloadStart : payloadStart+wire.CreditSize])
					payloadStart += wire.CreditSize
				}

				var owner frameOwner
				var payload []byte
				if frameEnd > payloadStart {
					body := s.pool.get(frameEnd - payloadStart)
					copy(*body, scratch[payloadStart:frameEnd])
					payload = *body
					owner = frameOwner{buf: body, pool: s.pool}
				}
				s.handlePSH(wire.Frame{
					Type:     hf.Type,
					Flags:    hf.Flags,
					StreamID: hf.StreamID,
					Credit:   credit,
					Payload:  payload,
				}, owner)
				if s.IsClosed() {
					return
				}
			} else {
				if err := s.handleControl(wire.Frame{
					Type:     hf.Type,
					Flags:    hf.Flags,
					StreamID: hf.StreamID,
					Payload:  scratch[frameStart+wire.HeaderSize : frameEnd],
				}); err != nil {
					s.fail(err)
					return
				}
				if s.IsClosed() {
					return
				}
			}
			off = frameEnd
		}

		if s.pendingReadErr != nil {
			err := s.pendingReadErr
			s.pendingReadErr = nil
			s.failRead(err)
			return
		}
		if off > 0 {
			copy(scratch[:], scratch[off:n])
			n -= off
			off = 0
		}
		if n == len(scratch) {
			s.fail(fmt.Errorf("%w: receive buffer stalled", ErrProtocol))
			return
		}

		r, err := s.conn.Read(scratch[n:])
		if r > 0 {
			n += r
			s.lastRecv.Store(time.Now().UnixNano())
		}
		if err != nil {
			if r > 0 {
				// Parse the complete frames that arrived with the error before
				// terminating the session.
				s.pendingReadErr = err
				continue
			}
			s.failRead(err)
			return
		}
	}
}

// recvLoopBatched reads up to a full block per syscall and parses as many
// frames as it contains. Payloads reference the block directly; a block is
// recycled only after the reader and every queued payload release it.
func (s *Session) recvLoopBatched() {
	defer s.loops.Done()

	block := s.blockPool.small.get()
	defer func() {
		block.release()
	}()
	n, off := 0, 0

	for {
		if s.IsClosed() {
			return
		}
		if n-off < wire.HeaderSize {
			if !s.makeRoom(&block, &n, &off) {
				return
			}
			continue
		}

		hf, total, err := wire.DecodeHeader(block.data[off:n], s.recvMaxFrame)
		if err != nil {
			s.fail(fmt.Errorf("%w: %v", ErrProtocol, err))
			return
		}
		if !s.peerSettingsSet.Load() && hf.Type != wire.TypeSETTINGS {
			s.fail(fmt.Errorf("%w: first frame is %s, want SETTINGS", ErrProtocol, hf.Type))
			return
		}
		if total > len(block.data) {
			if !s.growBlock(&block, &n, &off, total) {
				return
			}
			continue
		}
		if n-off < total {
			if !s.makeRoom(&block, &n, &off) {
				return
			}
			continue
		}

		frameStart := off
		frameEnd := off + total
		if hf.Type == wire.TypePSH {
			var credit uint32
			payloadStart := frameStart + wire.HeaderSize
			if hf.Flags&wire.FlagCredit != 0 {
				credit = binary.LittleEndian.Uint32(block.data[payloadStart : payloadStart+wire.CreditSize])
				payloadStart += wire.CreditSize
			}
			payload := block.data[payloadStart:frameEnd]
			var owner frameOwner
			if len(payload) > 0 {
				block.retain()
				owner = frameOwner{block: block}
			}
			s.handlePSH(wire.Frame{
				Type:     hf.Type,
				Flags:    hf.Flags,
				StreamID: hf.StreamID,
				Credit:   credit,
				Payload:  payload,
			}, owner)
			if s.IsClosed() {
				return
			}
		} else {
			f := wire.Frame{
				Type:     hf.Type,
				Flags:    hf.Flags,
				StreamID: hf.StreamID,
				Payload:  block.data[frameStart+wire.HeaderSize : frameEnd],
			}
			if err := s.handleControl(f); err != nil {
				s.fail(err)
				return
			}
			if s.IsClosed() {
				return
			}
		}

		off = frameEnd
		if off == n {
			if block.pool == s.blockPool.small && block.refs.Load() == 1 {
				n, off = 0, 0
				if !s.readMore(block, &n) {
					return
				}
				continue
			}
			block.release()
			block = s.blockPool.small.get()
			n, off = 0, 0
			if !s.readMore(block, &n) {
				return
			}
			continue
		}

		leftLen := n - off
		if block.pool == s.blockPool.large && leftLen <= s.blockPool.small.size {
			nb := s.blockPool.small.get()
			copy(nb.data, block.data[off:n])
			block.release()
			block = nb
			n, off = leftLen, 0
			continue
		}

		if block.refs.Load() == 1 {
			copy(block.data, block.data[off:n])
			n -= off
			off = 0
			continue
		}

		// The block is retained by queued payloads; move the partial frame to
		// a fresh block so the old one can be recycled independently.
		nb := block.pool.get()
		copy(nb.data, block.data[off:n])
		nn := n - off
		block.release()
		block = nb
		n, off = nn, 0
	}
}

// growBlock replaces a small block with a large one when the frame header
// announces a payload that does not fit.
func (s *Session) growBlock(block **frameBlock, n, off *int, total int) bool {
	b := *block
	if total > s.blockPool.large.size {
		s.fail(fmt.Errorf("%w: frame of %d bytes exceeds read block of %d", ErrProtocol, total, s.blockPool.large.size))
		return false
	}

	nb := s.blockPool.large.get()
	copy(nb.data, b.data[*off:*n])
	nn := *n - *off
	b.release()
	*block = nb
	*n = nn
	*off = 0
	return s.readMore(nb, n)
}

// makeRoom compacts a partial frame to the front of the block and reads more
// data, or copies it to a fresh block when the current one is retained.
func (s *Session) makeRoom(block **frameBlock, n, off *int) bool {
	b := *block
	if b.refs.Load() == 1 {
		copy(b.data, b.data[*off:*n])
		*n -= *off
		*off = 0
		return s.readMore(b, n)
	}

	nbPool := b.pool
	if nbPool == s.blockPool.large && (*n-*off) <= s.blockPool.small.size {
		nbPool = s.blockPool.small
	}
	nb := nbPool.get()
	copy(nb.data, b.data[*off:*n])
	nn := *n - *off
	b.release()
	*block = nb
	*n = nn
	*off = 0
	return s.readMore(nb, n)
}

func (s *Session) readMore(block *frameBlock, n *int) bool {
	if s.pendingReadErr != nil {
		err := s.pendingReadErr
		s.pendingReadErr = nil
		s.failRead(err)
		return false
	}
	if *n >= len(block.data) {
		s.fail(fmt.Errorf("%w: receive block overflow", ErrProtocol))
		return false
	}
	r, err := s.conn.Read(block.data[*n:])
	if r > 0 {
		*n += r
		s.lastRecv.Store(time.Now().UnixNano())
	}
	if err != nil {
		if r > 0 {
			// Keep the bytes and let recvLoopBatched parse complete frames
			// before failRead terminates the session.
			s.pendingReadErr = err
			return true
		}
		s.failRead(err)
		return false
	}
	return true
}

func (s *Session) keepaliveLoop() {
	defer s.loops.Done()

	ping := time.NewTicker(s.config.KeepAliveInterval)
	checkInterval := s.config.KeepAliveTimeout / 2
	if checkInterval < time.Millisecond {
		checkInterval = time.Millisecond
	}
	timeout := time.NewTicker(checkInterval)
	defer ping.Stop()
	defer timeout.Stop()

	for {
		select {
		case <-ping.C:
			nonce := s.pingNonce.Add(1)
			s.pingSentAt.Store(time.Now().UnixNano())
			s.enqueueCtrl(wire.NewPINGFrame(nonce))
		case <-timeout.C:
			last := s.lastRecv.Load()
			if last == 0 || time.Duration(time.Now().UnixNano()-last) >= s.config.KeepAliveTimeout {
				s.fail(ErrKeepAliveTimeout)
				return
			}
		case <-s.die:
			return
		}
	}
}
