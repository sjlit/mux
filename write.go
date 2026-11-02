package mux

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sjlit/mux/wire"
)

// writeBatch is one frame submitted to the single writer. A Write call may
// enqueue up to Config.WritePipeline batches before waiting for completions.
type writeBatch struct {
	stream *Stream
	data   []byte
	flags  wire.Flags
	done   chan struct{}
	n      int
	err    error

	canceled  atomic.Bool
	written   atomic.Bool
	completed atomic.Bool
}

var (
	writeBatchPool = sync.Pool{New: func() any { return new(writeBatch) }}
	doneChanPool   = sync.Pool{New: func() any { return make(chan struct{}, 1) }}
	buffersPool    = sync.Pool{New: func() any { return new(net.Buffers) }}
)

func getDoneChan() chan struct{} {
	return doneChanPool.Get().(chan struct{})
}

func newWriteBatch(st *Stream, data []byte, flags wire.Flags, done chan struct{}) *writeBatch {
	b := writeBatchPool.Get().(*writeBatch)
	b.stream = st
	b.data = data
	b.flags = flags
	b.done = done
	return b
}

func (b *writeBatch) reset() {
	b.stream = nil
	b.data = nil
	b.flags = 0
	b.done = nil
	b.n = 0
	b.err = nil
	b.canceled.Store(false)
	b.written.Store(false)
	b.completed.Store(false)
}

func (b *writeBatch) release() {
	if b.done != nil {
		select {
		case <-b.done:
		default:
		}
		doneChanPool.Put(b.done)
	}
	b.reset()
	writeBatchPool.Put(b)
}

func (b *writeBatch) complete(n int, err error) {
	if !b.completed.CompareAndSwap(false, true) {
		return
	}
	b.n = n
	b.err = err
	if b.done != nil {
		b.done <- struct{}{}
	}
}

// writerIdle reports whether the batching writer has nothing pending. Control
// frames are included so a fast-path data frame cannot overtake SETTINGS or a
// stream teardown queued by another goroutine.
func (s *Session) writerIdle() bool {
	if s.IsClosed() {
		return false
	}
	// The direct path must not emit stream data before our SETTINGS frame.
	if !s.settingsSent.Load() {
		return false
	}
	if s.ctrlLen() != 0 {
		return false
	}
	s.sendMu.Lock()
	dataEmpty := len(s.active) == 0
	s.sendMu.Unlock()
	return dataEmpty
}

// Write writes b to the stream. Large writes are split into frames and up to
// Config.WritePipeline frames may be in flight at once, allowing the writer to
// batch them into a single writev.
func (st *Stream) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	st.writeMu.Lock()
	defer st.writeMu.Unlock()

	if err := st.checkWriteState(); err != nil {
		return 0, err
	}

	depth := st.sess.config.WritePipeline
	if depth < 1 {
		depth = 1
	}
	var outBuf [64]*writeBatch
	if depth > len(outBuf) {
		depth = len(outBuf)
	}
	out := outBuf[:0]
	var pendingBuf [64]*writeBatch
	pending := pendingBuf[:0]
	pendingBytes := 0
	written := 0
	burstLimit := st.sess.config.WriteBatchSize + st.sess.sendFrameSize()

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}

		// When the writer is idle and this Write has no earlier queued
		// batches, the caller can write the whole burst itself. This avoids
		// two goroutine wakeups per Write and keeps a single-writer workload
		// on one writev per burst.
		if len(out) == 0 && st.sess.writerIdle() && len(pending) <= len(st.sess.directInfos) {
			sess := st.sess
			sess.directMu.Lock()
			hdrArena := sess.directHdr[:0]
			vec := sess.directVec[:0]
			infos := sess.directInfos[:0]
			ok := sess.writeFrames(nil, pending, &hdrArena, &vec, &infos)
			sess.directMu.Unlock()
			var firstErr error
			for _, batch := range pending {
				written += batch.n
				if batch.err != nil && firstErr == nil {
					firstErr = batch.err
				}
				batch.release()
			}
			pending = pending[:0]
			pendingBytes = 0
			if firstErr != nil {
				return firstErr
			}
			if !ok {
				if err := st.sess.closeError(); err != nil {
					return err
				}
				return ErrSessionClosed
			}
			return nil
		}

		for _, batch := range pending {
			batch.done = getDoneChan()
		}
		st.sess.enqueueBatches(st, pending)
		out = append(out, pending...)
		pending = pending[:0]
		pendingBytes = 0
		return nil
	}
	discard := func() {
		for _, batch := range pending {
			st.refund(len(batch.data))
			batch.release()
		}
		pending = pending[:0]
		pendingBytes = 0
	}
	fail := func(err error) (int, error) {
		discard()
		st.cancelBatches(out)
		written += st.drainBatches(out)
		out = nil
		return written, err
	}

	for len(b) > 0 {
		deadline := st.writeDeadlineTime()
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return fail(ErrTimeout)
		}

		if len(out) >= depth {
			oldest := out[0]
			n, err := st.awaitBatch(oldest, time.Time{}, true)
			oldest.release()
			written += n
			out = out[1:]
			if err != nil {
				return fail(err)
			}
		}

		if err := st.checkWriteState(); err != nil {
			return fail(err)
		}

		// Collect a burst of frames and flush it in one go. Keeping the burst
		// together prevents the writer from observing only the first frame of
		// a multi-frame Write and issuing one syscall per frame.
		for len(b) > 0 && len(out)+len(pending) < depth && pendingBytes < burstLimit {
			n := st.available(len(b))
			if n == 0 {
				break
			}
			st.reserve(n)
			batch := newWriteBatch(st, b[:n], 0, nil)
			pending = append(pending, batch)
			pendingBytes += wire.HeaderSize + n
			b = b[n:]
		}
		if len(pending) > 0 {
			if err := flush(); err != nil {
				return fail(err)
			}
			continue
		}

		if err := st.waitCredit(); err != nil {
			return fail(err)
		}
	}

	n, err := st.finishBatches(out)
	return written + n, err
}

// finishBatches waits for the last batch and then for the earlier ones. The
// sender completes batches of one stream in order, so if the last batch
// succeeded every earlier batch is already complete; the common path only
// needs one wakeup per Write instead of one per frame.
func (st *Stream) finishBatches(out []*writeBatch) (int, error) {
	if len(out) == 0 {
		return 0, nil
	}

	last := out[len(out)-1]
	n, err := st.awaitBatch(last, time.Time{}, true)
	last.release()
	if err != nil {
		st.cancelBatches(out[:len(out)-1])
		total := n
		for _, b := range out[:len(out)-1] {
			<-b.done
			total += b.n
			b.release()
		}
		return total, err
	}

	total := n
	for _, b := range out[:len(out)-1] {
		<-b.done
		total += b.n
		if b.err != nil && err == nil {
			err = b.err
		}
		b.release()
	}
	return total, err
}

func (st *Stream) available(max int) int {
	if max <= 0 {
		return 0
	}
	n := st.sess.sendFrameSize()
	if n > max {
		n = max
	}
	if a := st.streamAvailable(); a < n {
		n = a
	}
	if a := st.sess.sessionAvailable(); a < n {
		n = a
	}
	if n < 0 {
		return 0
	}
	return n
}

func (st *Stream) waitCredit() error {
	for {
		if err := st.checkWriteState(); err != nil {
			return err
		}
		if st.available(1) > 0 {
			return nil
		}

		// Register before reading the deadline/credit state so a concurrent
		// SetWriteDeadline or credit update cannot be lost.
		wake := st.sess.creditNotify.wait()
		deadline := st.writeDeadlineTime()
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return ErrTimeout
		}
		if st.available(1) > 0 {
			return nil
		}

		var timer *time.Timer
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timeout = timer.C
		}

		select {
		case <-wake:
		case <-timeout:
		case <-st.writeStop:
		case <-st.die:
		case <-st.sess.die:
		}
		stopTimer(timer)
	}
}

// awaitBatch waits for b to complete. When watch is true, the effective
// deadline is re-read from the stream on every wakeup, so SetWriteDeadline can
// shorten, extend, or clear a deadline for an already blocked Write. CloseWrite
// passes an explicit close timeout and watch=false.
func (st *Stream) awaitBatch(b *writeBatch, deadline time.Time, watch bool) (int, error) {
	for {
		// Register before reading the deadline so a concurrent SetWriteDeadline
		// cannot be lost between the read and wait.
		wake := st.writeDeadlineNotify.wait()
		if watch {
			deadline = st.writeDeadlineTime()
		}
		if deadline.IsZero() {
			select {
			case <-b.done:
				return b.n, b.err
			case <-wake:
				continue
			case <-st.writeStop:
				b.canceled.Store(true)
				st.sess.signal()
				<-b.done
				return st.batchResult(b, ErrWriteClosed)
			case <-st.die:
				b.canceled.Store(true)
				st.sess.signal()
				<-b.done
				return st.batchResult(b, st.getErr())
			case <-st.sess.die:
				b.canceled.Store(true)
				st.sess.signal()
				<-b.done
				return st.batchResult(b, st.sess.closeError())
			}
		}

		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-b.done:
			stopTimer(timer)
			return b.n, b.err
		case <-wake:
			stopTimer(timer)
			continue
		case <-timer.C:
			stopTimer(timer)
			b.canceled.Store(true)
			st.sess.signal()
			<-b.done
			if b.err != nil {
				return b.n, b.err
			}
			if b.n == len(b.data) {
				return b.n, nil
			}
			return b.n, ErrTimeout
		case <-st.writeStop:
			stopTimer(timer)
			b.canceled.Store(true)
			st.sess.signal()
			<-b.done
			return st.batchResult(b, ErrWriteClosed)
		case <-st.die:
			stopTimer(timer)
			b.canceled.Store(true)
			st.sess.signal()
			<-b.done
			return st.batchResult(b, st.getErr())
		case <-st.sess.die:
			stopTimer(timer)
			b.canceled.Store(true)
			st.sess.signal()
			<-b.done
			return st.batchResult(b, st.sess.closeError())
		}
	}
}

func (st *Stream) batchResult(b *writeBatch, fallback error) (int, error) {
	if b.err != nil {
		return b.n, b.err
	}
	if b.n == len(b.data) {
		return b.n, nil
	}
	return b.n, fallback
}

func (st *Stream) cancelBatches(out []*writeBatch) {
	for _, b := range out {
		b.canceled.Store(true)
	}
	st.sess.signal()
}

func (st *Stream) drainBatches(out []*writeBatch) int {
	n := 0
	for _, b := range out {
		<-b.done
		n += b.n
		b.release()
	}
	return n
}

func (s *Session) enqueueBatch(st *Stream, b *writeBatch) {
	s.enqueueBatches(st, []*writeBatch{b})
}

// enqueueBatches appends a burst of frames from one stream under a single lock
// and wakes the writer once. Keeping the burst together lets the writer put all
// frames of one Write into one writev instead of racing the enqueue loop.
func (s *Session) enqueueBatches(st *Stream, batches []*writeBatch) {
	if len(batches) == 0 {
		return
	}

	s.sendMu.Lock()
	if s.IsClosed() {
		err := s.closeError()
		s.sendMu.Unlock()
		for _, b := range batches {
			if !b.written.Load() {
				st.refund(len(b.data))
			}
			b.complete(0, err)
		}
		return
	}
	st.sendQ = append(st.sendQ, batches...)
	if !st.inActive {
		st.inActive = true
		s.active = append(s.active, st)
	}
	s.sendMu.Unlock()
	s.signal()
}

func (s *Session) takeData(data, canceled []*writeBatch, maxFrames, maxBytes int) ([]*writeBatch, []*writeBatch) {
	data = data[:0]
	canceled = canceled[:0]
	s.sendMu.Lock()
	bytes := 0
	for len(s.active) > 0 && len(data) < maxFrames {
		if s.rr >= len(s.active) {
			s.rr = 0
		}
		st := s.active[s.rr]

		if st.sendHead >= len(st.sendQ) {
			st.sendQ = st.sendQ[:0]
			st.sendHead = 0
			st.inActive = false
			s.removeActiveAt(s.rr)
			continue
		}

		b := st.sendQ[st.sendHead]
		st.sendQ[st.sendHead] = nil
		st.sendHead++
		if st.sendHead == len(st.sendQ) {
			st.sendQ = st.sendQ[:0]
			st.sendHead = 0
			st.inActive = false
			s.removeActiveAt(s.rr)
		} else {
			s.rr++
			if s.rr >= len(s.active) {
				s.rr = 0
			}
		}

		if b.canceled.Load() || st.aborted.Load() {
			canceled = append(canceled, b)
			continue
		}
		data = append(data, b)
		bytes += wire.HeaderSize + len(b.data)
		if len(data) > 0 && bytes >= maxBytes {
			break
		}
	}
	s.sendMu.Unlock()
	return data, canceled
}

func (s *Session) removeActiveAt(i int) {
	last := len(s.active) - 1
	s.active[i] = s.active[last]
	s.active[last] = nil
	s.active = s.active[:last]
	if s.rr >= len(s.active) {
		s.rr = 0
	}
}

func (s *Session) finishCanceled(b *writeBatch) {
	if !b.written.Load() {
		b.stream.refund(len(b.data))
	}
	err := error(ErrTimeout)
	if b.stream.aborted.Load() {
		err = b.stream.getErr()
	}
	if s.IsClosed() {
		err = s.closeError()
	}
	b.complete(0, err)
}

func (s *Session) sendLoop() {
	defer s.loops.Done()
	defer func() {
		s.drainSends(s.closeError())
	}()

	var hdrArena []byte
	var vec [][]byte
	var infos []frameInfo
	var ctrlBuf []ctrlMsg
	var dataBuf, canceledBuf []*writeBatch

	for {
		if s.IsClosed() {
			return
		}

		ctrl := s.takeCtrl(ctrlBuf[:0], 16)
		ctrlBuf = ctrl
		// Allow one extra frame beyond the soft batch limit so a Write that
		// ends in a short tail frame is still sent with one writev.
		maxBytes := s.config.WriteBatchSize + s.sendFrameSize()
		data, canceled := s.takeData(dataBuf[:0], canceledBuf[:0], 64, maxBytes)
		dataBuf, canceledBuf = data, canceled
		for _, b := range canceled {
			s.finishCanceled(b)
		}
		if len(ctrl) == 0 && len(data) == 0 {
			select {
			case <-s.wake:
				continue
			case <-s.die:
				return
			}
		}
		if !s.writeFrames(ctrl, data, &hdrArena, &vec, &infos) {
			return
		}
	}
}

type frameInfo struct {
	size  int
	batch *writeBatch
}

// writeFrames writes one batch (control frames first, then data frames) with a
// single scatter-gather call when the connection supports it.
func (s *Session) writeFrames(ctrl []ctrlMsg, data []*writeBatch, hdrArena *[]byte, vec *[][]byte, infos *[]frameInfo) bool {
	totalFrames := len(ctrl) + len(data)
	if totalFrames == 0 {
		return true
	}

	need := totalFrames * wire.HeaderSize
	if cap(*hdrArena) < need {
		*hdrArena = make([]byte, need)
	} else {
		*hdrArena = (*hdrArena)[:need]
	}
	*vec = (*vec)[:0]
	*infos = (*infos)[:0]
	clearVec := func() {
		for i := range *vec {
			(*vec)[i] = nil
		}
		*vec = (*vec)[:0]
	}

	totalBytes := 0
	ctrlBytes := 0

	failBuild := func(err error) bool {
		clearVec()
		// Complete every data batch, not only those already recorded in
		// infos: a header build can fail before the current batch is added.
		for _, b := range data {
			b.complete(0, err)
		}
		for _, c := range ctrl {
			if c.done != nil {
				close(c.done)
			}
		}
		for i := range *infos {
			(*infos)[i] = frameInfo{}
		}
		*infos = (*infos)[:0]
		s.fail(err)
		return false
	}

	i := 0
	hasSettings := false
	for _, c := range ctrl {
		if c.frame.Type == wire.TypeSETTINGS {
			hasSettings = true
		}
		base := (*hdrArena)[i*wire.HeaderSize : (i+1)*wire.HeaderSize]
		hdr, err := wire.AppendHeader(base[:0], c.frame)
		if err != nil {
			return failBuild(err)
		}
		*vec = append(*vec, hdr)
		totalBytes += len(hdr)
		ctrlBytes += len(hdr)
		if len(c.frame.Payload) != 0 {
			*vec = append(*vec, c.frame.Payload)
			totalBytes += len(c.frame.Payload)
			ctrlBytes += len(c.frame.Payload)
		}
		i++
	}

	for _, b := range data {
		f := wire.Frame{
			Type:     wire.TypePSH,
			Flags:    b.flags,
			StreamID: b.stream.id,
			Payload:  b.data,
		}
		base := (*hdrArena)[i*wire.HeaderSize : (i+1)*wire.HeaderSize]
		hdr, err := wire.AppendHeader(base[:0], f)
		if err != nil {
			return failBuild(err)
		}
		*vec = append(*vec, hdr)
		totalBytes += len(hdr)
		if len(b.data) != 0 {
			*vec = append(*vec, b.data)
			totalBytes += len(b.data)
		}
		*infos = append(*infos, frameInfo{size: wire.HeaderSize + len(b.data), batch: b})
		i++
	}

	for _, b := range data {
		if n := len(b.data); n > 0 {
			b.stream.numWritten.Add(uint32(n))
			s.sessionNumWritten.Add(uint32(n))
			b.stream.reserved.Add(0 - uint32(n))
			s.sessionReserved.Add(0 - uint32(n))
		}
		b.written.Store(true)
	}

	s.connMu.Lock()
	n, err := s.writeVec(*vec)
	s.connMu.Unlock()
	// Do not retain payload slices (which may point into caller buffers) in
	// the reusable iovec arena while the session is idle.
	clearVec()
	for _, c := range ctrl {
		if c.done != nil {
			close(c.done)
		}
	}

	if err == nil && n != totalBytes {
		err = io.ErrShortWrite
	}
	if err == nil && hasSettings {
		s.settingsSent.Store(true)
	}

	// n covers control frames before data frames. Subtract the control bytes
	// so a partial write in the control prefix cannot be mistaken for data.
	remain := n - ctrlBytes
	var fullErr error
	if n >= totalBytes {
		fullErr = err
	}
	for idx, info := range *infos {
		b := info.batch
		payload := len(b.data)
		if remain >= info.size {
			b.complete(payload, fullErr)
			remain -= info.size
			continue
		}
		partial := remain - wire.HeaderSize
		if partial < 0 {
			partial = 0
		}
		if partial > payload {
			partial = payload
		}
		b.complete(partial, err)
		remain = 0
		for j := idx + 1; j < len(*infos); j++ {
			(*infos)[j].batch.complete(0, err)
		}
		break
	}
	for i := range *infos {
		(*infos)[i] = frameInfo{}
	}
	*infos = (*infos)[:0]

	if err != nil {
		s.fail(err)
		return false
	}
	return true
}

func (s *Session) writeVec(vec [][]byte) (int, error) {
	if bw, ok := s.conn.(interface {
		WriteBuffers([][]byte) (int, error)
	}); ok {
		return bw.WriteBuffers(vec)
	}
	nb := buffersPool.Get().(*net.Buffers)
	*nb = vec
	n, err := nb.WriteTo(s.conn)
	*nb = nil
	buffersPool.Put(nb)
	return int(n), err
}

func (s *Session) drainSends(err error) {
	if err == nil {
		err = ErrSessionClosed
	}

	s.sendMu.Lock()
	active := s.active
	s.active = nil
	s.rr = 0
	var batches []*writeBatch
	for _, st := range active {
		st.inActive = false
		for i := st.sendHead; i < len(st.sendQ); i++ {
			batches = append(batches, st.sendQ[i])
			st.sendQ[i] = nil
		}
		st.sendQ = st.sendQ[:0]
		st.sendHead = 0
	}
	s.sendMu.Unlock()

	for _, b := range batches {
		if !b.written.Load() {
			b.stream.refund(len(b.data))
		}
		b.complete(0, err)
	}

	s.ctrlMu.Lock()
	ctrl := append([]ctrlMsg(nil), s.ctrlQ[s.ctrlHead:]...)
	s.ctrlQ = nil
	s.ctrlHead = 0
	s.pendingRST = nil
	s.ctrlMu.Unlock()
	for _, c := range ctrl {
		if c.done != nil {
			close(c.done)
		}
	}
}
