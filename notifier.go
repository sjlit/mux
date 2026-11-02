package mux

import "sync"

// notifier is a broadcast primitive. Each notification closes the current
// channel and installs a fresh one lazily, so every waiter that captured the
// previous channel is woken. Callers must capture the channel before
// re-checking the condition they are waiting on.
//
// notify is a no-op when nobody is waiting, which avoids allocating a channel
// for every received frame.
type notifier struct {
	mu sync.Mutex
	ch chan struct{}
}

func newNotifier() *notifier {
	return &notifier{}
}

func (n *notifier) wait() <-chan struct{} {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	ch := n.ch
	n.mu.Unlock()
	return ch
}

func (n *notifier) notify() {
	if n == nil {
		return
	}
	n.mu.Lock()
	if n.ch != nil {
		close(n.ch)
		n.ch = nil
	}
	n.mu.Unlock()
}

// signal is a reusable wakeup channel for a single data path. A token is only
// delivered when one is not already pending, so notifying a busy stream costs
// no allocation. Broadcast is not required: a reader that drains one frame
// notifies again when more data remains, and terminal states are delivered
// through the fin and die channels.
type signal struct {
	ch chan struct{}
}

func newSignal() *signal {
	return &signal{ch: make(chan struct{}, 1)}
}

func (s *signal) wait() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ch
}

func (s *signal) notify() {
	if s == nil {
		return
	}
	select {
	case s.ch <- struct{}{}:
	default:
	}
}
