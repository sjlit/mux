package mux

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sjlit/mux/wire"
)

// maxBufferedFrames and maxSessionBufferedFrames bound discrete PSH payloads
// queued for one stream and across the session. Flow control bounds bytes, but
// a peer can send one-byte frames and make every byte cost a ring entry; these
// caps bound that amplification.
const (
	maxBufferedFrames        = 64 << 10
	maxSessionBufferedFrames = 128 << 10
)

var errStreamBufferFull = errors.New("mux: stream receive buffer full")

// Stream is a logical bidirectional byte stream multiplexed over a Session.
type Stream struct {
	id    uint32
	sess  *Session
	local bool

	// Send-side flow control. Peer counters are absolute; reservations cover
	// frames that have been admitted but not yet written.
	peerWindow   atomic.Uint32
	peerConsumed atomic.Uint32
	numWritten   atomic.Uint32
	reserved     atomic.Uint32

	writeMu sync.Mutex // serializes Write calls

	// submitMu orders data submissions with FIN/RST. It is held only while
	// enqueueing a burst, never while waiting for it to be written, so Close
	// can queue a teardown frame even if a concurrent Write is blocked in
	// conn.Write.
	submitMu sync.Mutex

	sendQ       []*writeBatch // protected by sess.sendMu
	sendHead    int           // protected by sess.sendMu
	inActive    bool          // protected by sess.sendMu
	closing     atomic.Bool
	writeClosed atomic.Bool
	finQueued   atomic.Bool
	writeStop   chan struct{}
	stopOnce    sync.Once

	// Receive-side buffering and flow control.
	bufferLock       sync.Mutex
	ring             *bufferRing
	recvBytes        atomic.Uint32
	consumed         atomic.Uint32
	recvWindow       uint32
	updMu            sync.Mutex
	lastSentConsumed atomic.Uint32

	// Lifecycle.
	die               chan struct{}
	dieOnce           sync.Once
	errMu             sync.Mutex
	err               error
	drainErr          atomic.Pointer[error]
	acceptMu          sync.Mutex
	aborted           atomic.Bool
	removed           atomic.Bool
	remoteFIN         atomic.Bool
	finOnce           sync.Once
	finCh             chan struct{}
	accepted          atomic.Bool
	resetBeforeAccept atomic.Bool

	readDeadline        atomic.Value
	writeDeadline       atomic.Value
	readNotify          *signal
	readDeadlineNotify  *notifier
	writeDeadlineNotify *notifier
}

func newStream(sess *Session, id uint32, local bool, peerWindow uint32) *Stream {
	st := &Stream{
		id:                  id,
		sess:                sess,
		local:               local,
		die:                 make(chan struct{}),
		writeStop:           make(chan struct{}),
		finCh:               make(chan struct{}),
		readNotify:          newSignal(),
		readDeadlineNotify:  newNotifier(),
		writeDeadlineNotify: newNotifier(),
		recvWindow:          sess.recvStreamWindow,
	}
	st.peerWindow.Store(peerWindow)
	if local {
		st.accepted.Store(true)
	}
	return st
}

// ID returns the stream identifier.
func (st *Stream) ID() uint32 {
	return st.id
}

// LocalAddr satisfies net.Conn.
func (st *Stream) LocalAddr() net.Addr {
	return st.sess.LocalAddr()
}

// RemoteAddr satisfies net.Conn.
func (st *Stream) RemoteAddr() net.Addr {
	return st.sess.RemoteAddr()
}

func (st *Stream) setErr(err error) {
	if err == nil {
		return
	}
	st.errMu.Lock()
	if st.err == nil {
		st.err = err
	}
	st.errMu.Unlock()
}

func (st *Stream) getErr() error {
	st.errMu.Lock()
	err := st.err
	st.errMu.Unlock()
	if err == nil {
		return ErrStreamClosed
	}
	return err
}

func (st *Stream) setDrainErr(err error) {
	if err == nil {
		return
	}
	e := err
	st.drainErr.CompareAndSwap(nil, &e)
}

func (st *Stream) getDrainErr() error {
	p := st.drainErr.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (st *Stream) closeDie() {
	st.dieOnce.Do(func() {
		close(st.die)
	})
}

func (st *Stream) readError() error {
	select {
	case <-st.die:
		return st.getErr()
	default:
	}
	if st.sess.IsClosed() {
		return st.sess.closeError()
	}
	return nil
}

// Read reads data from the stream.
func (st *Stream) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	for {
		if st.getDrainErr() == nil {
			if err := st.readError(); err != nil {
				return 0, err
			}
		}
		deadline := st.readDeadlineTime()
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, ErrTimeout
		}

		n, err := st.tryRead(b)
		if err != ErrWouldBlock {
			return n, err
		}

		wake := st.readNotify.wait()
		deadlineWake := st.readDeadlineNotify.wait()
		// Re-read after registering for deadline changes; a deadline set
		// between the first tryRead and wait() must not be lost.
		deadline = st.readDeadlineTime()
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, ErrTimeout
		}
		n, err = st.tryRead(b)
		if err != ErrWouldBlock {
			return n, err
		}

		// Advertise everything consumed so far before sleeping. The regular
		// half-window update policy can otherwise leave the peer blocked: an
		// application may consume part of a stream, then wait for more data
		// while the session window is exhausted by other streams.
		st.flushConsumed()

		var timer *time.Timer
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timeout = timer.C
		}

		select {
		case <-wake:
		case <-deadlineWake:
		case <-timeout:
		case <-st.finCh:
		case <-st.die:
		case <-st.sess.die:
		}
		stopTimer(timer)
	}
}

func (st *Stream) readDeadlineTime() time.Time {
	if d, ok := st.readDeadline.Load().(time.Time); ok {
		return d
	}
	return time.Time{}
}

func (st *Stream) writeDeadlineTime() time.Time {
	if d, ok := st.writeDeadline.Load().(time.Time); ok {
		return d
	}
	return time.Time{}
}

func (st *Stream) tryRead(b []byte) (int, error) {
	drainErr := st.getDrainErr()
	if drainErr == nil {
		if err := st.readError(); err != nil {
			return 0, err
		}
	}

	// Coalesce several queued frames into one Read. This matters when a
	// caller reads a large buffer: it avoids a lock and a wakeup per frame.
	var owners [8]frameOwner
	ownerCount := 0
	n := 0
	st.bufferLock.Lock()
	for n < len(b) && ownerCount < len(owners) {
		got, owner := st.ring.consumeFront(b[n:])
		if got == 0 {
			break
		}
		n += got
		if owner.block != nil || owner.buf != nil {
			owners[ownerCount] = owner
			ownerCount++
		}
	}
	more := st.ring.len() > 0
	st.bufferLock.Unlock()
	if ownerCount > 0 {
		st.sess.bufferedFrames.Add(-int64(ownerCount))
	}
	for i := 0; i < ownerCount; i++ {
		owners[i].release()
	}

	if n > 0 {
		st.consumed.Add(uint32(n))
		st.sess.sessionConsumed.Add(uint32(n))
		st.maybeSendStreamUPD()
		st.sess.publishSessionCredit()
		st.tryCleanup()
		if more {
			st.readNotify.notify()
		}
		return n, nil
	}

	if drainErr != nil {
		return 0, drainErr
	}
	if st.remoteFIN.Load() {
		return 0, io.EOF
	}
	return 0, ErrWouldBlock
}

func (st *Stream) push(data []byte, owner frameOwner) error {
	st.bufferLock.Lock()
	defer st.bufferLock.Unlock()

	select {
	case <-st.die:
		return ErrStreamClosed
	default:
	}
	if st.aborted.Load() {
		return ErrStreamClosed
	}
	if st.ring != nil && st.ring.len() >= maxBufferedFrames {
		return errStreamBufferFull
	}
	if st.ring == nil {
		st.ring = newBufferRing()
	}
	st.ring.push(data, owner)
	return nil
}

func (st *Stream) handleData(f wire.Frame, owner frameOwner) {
	n := len(f.Payload)

	if st.aborted.Load() {
		st.discardData(n, owner)
		return
	}
	if st.remoteFIN.Load() && n > 0 {
		owner.release()
		st.sess.fail(fmt.Errorf("%w: data after FIN on stream %d", ErrProtocol, st.id))
		return
	}
	if !st.checkRecvWindow(n) {
		owner.release()
		st.sess.fail(fmt.Errorf("%w: stream %d receive window exceeded", ErrFlowControl, st.id))
		return
	}
	if !st.sess.checkSessionWindow(n) {
		owner.release()
		st.sess.fail(fmt.Errorf("%w: session receive window exceeded", ErrFlowControl))
		return
	}

	if n > 0 {
		st.recvBytes.Add(uint32(n))
		st.sess.sessionRecvBytes.Add(uint32(n))
		if st.sess.bufferedFrames.Load() >= maxSessionBufferedFrames {
			owner.release()
			st.sess.fail(fmt.Errorf("%w: session receive queue full", ErrFlowControl))
			return
		}
		st.sess.bufferedFrames.Add(1)
		err := st.push(f.Payload, owner)
		if err == nil {
			st.readNotify.notify()
			st.sess.maybeSendSessionUPDOnPressure()
		} else {
			st.sess.bufferedFrames.Add(-1)
			owner.release()
			if errors.Is(err, errStreamBufferFull) {
				st.sess.fail(fmt.Errorf("%w: stream %d receive queue full", ErrFlowControl, st.id))
				return
			}
			st.consumed.Add(uint32(n))
			st.sess.sessionConsumed.Add(uint32(n))
			st.sess.publishSessionCredit()
		}
	} else {
		owner.release()
	}

	if f.Flags&wire.FlagFIN != 0 {
		st.remoteFIN.Store(true)
		st.finOnce.Do(func() { close(st.finCh) })
		st.tryCleanup()
	}
}

func (st *Stream) discardData(n int, owner frameOwner) {
	owner.release()
	if n <= 0 {
		return
	}
	st.recvBytes.Add(uint32(n))
	st.sess.sessionRecvBytes.Add(uint32(n))
	st.consumed.Add(uint32(n))
	st.sess.sessionConsumed.Add(uint32(n))
	st.sess.publishSessionCredit()
}

func (st *Stream) checkRecvWindow(n int) bool {
	inflight := int32(st.recvBytes.Load() - st.consumed.Load())
	if inflight < 0 {
		return false
	}
	return inflight+int32(n) <= int32(st.recvWindow)
}

func (st *Stream) applyPeerCredit(consumed uint32) {
	written := st.numWritten.Load()
	if itimediff(consumed, written) > 0 {
		st.sess.fail(fmt.Errorf("%w: stream %d consumed %d exceeds written %d", ErrFlowControl, st.id, consumed, written))
		return
	}
	if itimediff(consumed, st.peerConsumed.Load()) <= 0 {
		return
	}
	st.peerConsumed.Store(consumed)
	st.sess.creditNotify.notify()
}

func (st *Stream) maybeSendStreamUPD() {
	st.sendStreamUPD(false)
}

// sendStreamUPD advertises the absolute consumed counter. When force is true
// it ignores the half-window threshold; callers use it before blocking so a
// consumer that stops at a message boundary cannot leave the peer without
// credit.
func (st *Stream) sendStreamUPD(force bool) {
	if st.aborted.Load() || st.removed.Load() {
		return
	}
	consumed := st.consumed.Load()
	threshold := int32(st.recvWindow / 2)

	st.updMu.Lock()
	delta := itimediff(consumed, st.lastSentConsumed.Load())
	if delta <= 0 || (!force && delta < threshold) {
		st.updMu.Unlock()
		return
	}
	st.lastSentConsumed.Store(consumed)
	st.updMu.Unlock()
	st.sess.enqueueCtrl(wire.NewUPDFrame(st.id, consumed))
}

// flushConsumed publishes pending credit when the session receive window is
// under pressure. Sending unconditionally on every blocking Read would turn a
// request/response workload into one UPD frame per message; limiting it to
// window pressure still prevents the cross-stream head-of-line deadlock.
func (st *Stream) flushConsumed() {
	if !st.sess.sessionWindowPressure() {
		return
	}
	st.sendStreamUPD(true)
	st.sess.sendSessionUPD(true)
}

func (st *Stream) discardBuffered() {
	st.bufferLock.Lock()
	var owners []frameOwner
	var n int
	for st.ring != nil && st.ring.len() > 0 {
		data, owner, ok := st.ring.pop()
		if !ok {
			break
		}
		n += len(data)
		owners = append(owners, owner)
	}
	st.bufferLock.Unlock()

	if len(owners) > 0 {
		st.sess.bufferedFrames.Add(-int64(len(owners)))
	}
	for _, owner := range owners {
		owner.release()
	}
	if n > 0 {
		st.consumed.Add(uint32(n))
		st.sess.sessionConsumed.Add(uint32(n))
		// Closing/resetting a stream can release a large amount of buffered
		// data. Publish it immediately so other streams are not left blocked
		// on the session window.
		st.sess.sendSessionUPD(true)
	}
}

func (st *Stream) tryCleanup() {
	if st.removed.Load() || !st.remoteFIN.Load() || !st.finQueued.Load() {
		return
	}
	st.bufferLock.Lock()
	empty := st.ring == nil || st.ring.len() == 0
	st.bufferLock.Unlock()
	if empty {
		st.sess.removeStream(st)
	}
}

func (st *Stream) abortLocal(err error, sendRST bool, code wire.ErrorCode) {
	st.setErr(err)
	st.aborted.Store(true)
	st.closeDie()
	st.discardBuffered()
	st.readNotify.notify()
	st.sess.creditNotify.notify()
	if sendRST {
		// Submit the RST through the same gate as data. A direct write that
		// already holds connMu finishes first; one that has not reached the
		// write sees aborted/closing and drops its burst. Do not wait for
		// writeMu here: a Write blocked in conn.Write must not make Reset hang.
		st.submitMu.Lock()
		st.sess.enqueueCtrl(wire.NewRSTFrame(st.id, code))
		st.submitMu.Unlock()
	}
}

// remoteReset handles an RST from the peer.
func (st *Stream) remoteReset(code wire.ErrorCode) {
	st.stopOnce.Do(func() { close(st.writeStop) })
	st.acceptMu.Lock()
	if !st.accepted.Load() {
		st.resetBeforeAccept.Store(true)
	}
	st.acceptMu.Unlock()
	st.abortLocal(newStreamError(st.id, code), false, code)
	st.sess.removeStream(st)
}

// sessionClosed tears the stream down when the session dies.
func (st *Stream) sessionClosed(err error) {
	st.stopOnce.Do(func() { close(st.writeStop) })
	st.acceptMu.Lock()
	if !st.accepted.Load() {
		st.resetBeforeAccept.Store(true)
	}
	st.acceptMu.Unlock()
	st.abortLocal(err, false, wire.CodeCancel)
	st.sess.removeStream(st)
}

// sessionReadError terminates the stream after the transport read side fails,
// but keeps buffered payloads available to Read. Reads drain the ring first and
// then return err; writes fail immediately.
func (st *Stream) sessionReadError(err error) {
	st.setErr(err)
	st.setDrainErr(err)
	st.stopOnce.Do(func() { close(st.writeStop) })
	st.aborted.Store(true)
	st.closeDie()
	st.readNotify.notify()
	st.sess.creditNotify.notify()
	st.sess.removeStream(st)
}

// CloseWrite half-closes the stream after all queued data has been written.
// It waits at most CloseTimeout (or an earlier write deadline) for an
// in-flight Write to finish and for the FIN frame to be written.
func (st *Stream) CloseWrite() error {
	deadline := time.Now().Add(st.sess.config.CloseTimeout)
	if wd := st.writeDeadlineTime(); !wd.IsZero() && wd.Before(deadline) {
		deadline = wd
	}
	if !time.Now().Before(deadline) {
		return ErrTimeout
	}
	if !st.lockWriteUntil(deadline) {
		return ErrTimeout
	}
	if st.writeClosed.Load() || st.closing.Load() {
		st.writeMu.Unlock()
		return ErrWriteClosed
	}
	if st.aborted.Load() {
		err := st.getErr()
		st.writeMu.Unlock()
		return err
	}

	b := newWriteBatch(st, nil, wire.FlagFIN, getDoneChan())
	st.submitMu.Lock()
	if st.writeClosed.Load() || st.closing.Load() {
		st.submitMu.Unlock()
		st.writeMu.Unlock()
		b.release()
		return ErrWriteClosed
	}
	st.writeClosed.Store(true)
	st.finQueued.Store(true)
	st.sess.enqueueBatch(st, b)
	st.submitMu.Unlock()
	st.writeMu.Unlock()

	_, err, done := st.awaitFin(b, deadline)
	if done {
		b.release()
	}
	if err != nil {
		if done {
			st.submitMu.Lock()
			st.finQueued.Store(false)
			st.submitMu.Unlock()
		}
		return err
	}
	st.tryCleanup()
	return nil
}

// Close closes both directions of the stream. Already queued writes are
// flushed and followed by a FIN; the read side is abandoned immediately.
// If unread data remains and the peer may still send, Close sends RST(CANCEL)
// instead, as required by the protocol.
//
// Close no longer waits for writeMu before queueing the teardown frame, so a
// concurrent Write blocked in conn.Write cannot prevent Close from returning.
// The FIN itself is bounded by CloseTimeout; on timeout Close returns
// ErrTimeout and the FIN may still be written in the background.
func (st *Stream) Close() error {
	st.stopOnce.Do(func() { close(st.writeStop) })

	st.setErr(ErrStreamClosed)
	st.closeDie()
	unread := !st.ringEmpty() && !st.remoteFIN.Load()
	st.discardBuffered()
	st.readNotify.notify()
	st.sess.creditNotify.notify()
	st.closing.Store(true)

	var fin *writeBatch
	st.submitMu.Lock()
	st.writeClosed.Store(true)
	if unread {
		// The peer may keep sending; tell it to stop instead of relying on
		// unknown-stream discards after the stream is removed.
		st.aborted.Store(true)
		st.sess.enqueueCtrl(wire.NewRSTFrame(st.id, wire.CodeCancel))
	} else if !st.aborted.Load() && !st.finQueued.Load() {
		st.finQueued.Store(true)
		fin = newWriteBatch(st, nil, wire.FlagFIN, getDoneChan())
		st.sess.enqueueBatch(st, fin)
	}
	st.submitMu.Unlock()

	var err error
	if fin != nil {
		var done bool
		_, err, done = st.awaitFin(fin, time.Now().Add(st.sess.config.CloseTimeout))
		if done {
			fin.release()
		}
		if err != nil && done {
			st.submitMu.Lock()
			st.finQueued.Store(false)
			st.submitMu.Unlock()
		}
	}
	st.sess.removeStream(st)
	return err
}

// awaitFin waits for a FIN batch. On timeout it returns ErrTimeout without
// cancelling or waiting for the batch: the FIN has no payload, so it is safe
// to let the writer finish it in the background even after Close returns.
// The done result reports whether b was observed complete, so the caller may
// safely release it. On timeout the batch may still be referenced by the
// writer; it is left for the garbage collector rather than racing a pooled
// reuse.
func (st *Stream) awaitFin(b *writeBatch, deadline time.Time) (int, error, bool) {
	if deadline.IsZero() {
		select {
		case <-b.done:
			return b.n, b.err, true
		case <-st.sess.die:
			return 0, st.sess.closeError(), false
		}
	}

	timer := time.NewTimer(time.Until(deadline))
	defer stopTimer(timer)

	select {
	case <-b.done:
		return b.n, b.err, true
	case <-timer.C:
		return 0, ErrTimeout, false
	case <-st.sess.die:
		return 0, st.sess.closeError(), false
	}
}

// lockWriteUntil acquires writeMu, waiting at most until deadline. It is used
// only by CloseWrite, where blocking forever behind a peer-stalled Write would
// be worse than returning ErrTimeout.
func (st *Stream) lockWriteUntil(deadline time.Time) bool {
	if st.writeMu.TryLock() {
		return true
	}
	if deadline.IsZero() {
		st.writeMu.Lock()
		return true
	}
	if !time.Now().Before(deadline) {
		return false
	}

	timer := time.NewTimer(time.Until(deadline))
	defer stopTimer(timer)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			return false
		case <-ticker.C:
			if st.writeMu.TryLock() {
				return true
			}
		}
	}
}

// Reset aborts the stream and sends an RST with code. Unsent data is
// discarded; a direct write already inside conn.Write is ordered before the
// RST, so the peer never observes data after it.
func (st *Stream) Reset(code wire.ErrorCode) error {
	st.stopOnce.Do(func() { close(st.writeStop) })
	st.abortLocal(newStreamError(st.id, code), true, code)
	st.sess.removeStream(st)
	return nil
}

// CloseRead aborts the stream with CANCEL.
func (st *Stream) CloseRead() error {
	return st.Reset(wire.CodeCancel)
}

// SetReadDeadline sets the read deadline. A zero value disables it.
func (st *Stream) SetReadDeadline(t time.Time) error {
	st.readDeadline.Store(t)
	st.readDeadlineNotify.notify()
	return nil
}

// SetWriteDeadline sets the write deadline. A zero value disables it.
// A deadline cannot interrupt a conn.Write that is already in progress; close
// the session or set a deadline on the underlying connection for a hard stop.
func (st *Stream) SetWriteDeadline(t time.Time) error {
	st.writeDeadline.Store(t)
	st.writeDeadlineNotify.notify()
	st.sess.creditNotify.notify()
	return nil
}

// SetDeadline sets both deadlines.
func (st *Stream) SetDeadline(t time.Time) error {
	if err := st.SetReadDeadline(t); err != nil {
		return err
	}
	return st.SetWriteDeadline(t)
}

func (st *Stream) checkWriteState() error {
	if st.aborted.Load() {
		return st.getErr()
	}
	select {
	case <-st.writeStop:
		return ErrWriteClosed
	default:
	}
	if st.writeClosed.Load() || st.closing.Load() {
		return ErrWriteClosed
	}
	select {
	case <-st.die:
		return st.getErr()
	default:
	}
	if st.sess.IsClosed() {
		return st.sess.closeError()
	}
	return nil
}

func (st *Stream) streamAvailable() int {
	inflight := int32(st.numWritten.Load() + st.reserved.Load() - st.peerConsumed.Load())
	if inflight < 0 {
		return 0
	}
	avail := int32(st.peerWindow.Load()) - inflight
	if avail <= 0 {
		return 0
	}
	return int(avail)
}

func (st *Stream) reserve(n int) {
	if n <= 0 {
		return
	}
	st.reserved.Add(uint32(n))
	st.sess.sessionReserved.Add(uint32(n))
}

func (st *Stream) refund(n int) {
	if n <= 0 {
		return
	}
	st.reserved.Add(0 - uint32(n))
	st.sess.sessionReserved.Add(0 - uint32(n))
}

func (st *Stream) ringEmpty() bool {
	st.bufferLock.Lock()
	empty := st.ring == nil || st.ring.len() == 0
	st.bufferLock.Unlock()
	return empty
}

// bufferRing is an ordered FIFO of incoming payload buffers.
type bufferRing struct {
	bufs   [][]byte
	owners []frameOwner
	head   int
	tail   int
	size   int
}

func newBufferRing() *bufferRing {
	return &bufferRing{
		bufs:   make([][]byte, 8),
		owners: make([]frameOwner, 8),
	}
}

func (r *bufferRing) len() int {
	if r == nil {
		return 0
	}
	return r.size
}

func (r *bufferRing) grow() {
	newCap := len(r.bufs) * 2
	if newCap == 0 {
		newCap = 8
	}
	bufs := make([][]byte, newCap)
	owners := make([]frameOwner, newCap)
	for i := 0; i < r.size; i++ {
		idx := (r.head + i) % len(r.bufs)
		bufs[i] = r.bufs[idx]
		owners[i] = r.owners[idx]
	}
	r.bufs = bufs
	r.owners = owners
	r.head = 0
	r.tail = r.size
}

func (r *bufferRing) push(data []byte, owner frameOwner) {
	if r.size == len(r.bufs) {
		r.grow()
	}
	r.bufs[r.tail] = data
	r.owners[r.tail] = owner
	r.tail = (r.tail + 1) % len(r.bufs)
	r.size++
}

func (r *bufferRing) pop() ([]byte, frameOwner, bool) {
	if r.size == 0 {
		return nil, frameOwner{}, false
	}
	data := r.bufs[r.head]
	owner := r.owners[r.head]
	r.bufs[r.head] = nil
	r.owners[r.head] = frameOwner{}
	r.head = (r.head + 1) % len(r.bufs)
	r.size--
	return data, owner, true
}

func (r *bufferRing) consumeFront(b []byte) (int, frameOwner) {
	if r == nil || r.size == 0 {
		return 0, frameOwner{}
	}
	n := copy(b, r.bufs[r.head])
	r.bufs[r.head] = r.bufs[r.head][n:]
	if len(r.bufs[r.head]) != 0 {
		return n, frameOwner{}
	}
	owner := r.owners[r.head]
	r.bufs[r.head] = nil
	r.owners[r.head] = frameOwner{}
	r.head = (r.head + 1) % len(r.bufs)
	r.size--
	return n, owner
}

func stopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
