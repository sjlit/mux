package mux

import (
	"sync"
	"sync/atomic"
)

const streamShardCount = 32

type streamShard struct {
	mu sync.RWMutex
	m  map[uint32]*Stream
}

// streamTable is a sharded stream ID map. Lookups happen on every received
// frame, while inserts/removals happen on stream lifecycle events, so a
// read-mostly sharded map keeps recvLoop off the global session lock.
type streamTable struct {
	shards [streamShardCount]streamShard
	count  int32 // atomic
}

func newStreamTable() *streamTable {
	return &streamTable{}
}

func shardIndex(id uint32) uint32 {
	return id & (streamShardCount - 1)
}

func (t *streamTable) get(id uint32) *Stream {
	sh := &t.shards[shardIndex(id)]
	sh.mu.RLock()
	st := sh.m[id]
	sh.mu.RUnlock()
	return st
}

func (t *streamTable) add(id uint32, st *Stream) bool {
	sh := &t.shards[shardIndex(id)]
	sh.mu.Lock()
	if sh.m == nil {
		sh.m = make(map[uint32]*Stream)
	}
	if _, ok := sh.m[id]; ok {
		sh.mu.Unlock()
		return false
	}
	sh.m[id] = st
	sh.mu.Unlock()
	atomic.AddInt32(&t.count, 1)
	return true
}

func (t *streamTable) del(id uint32) *Stream {
	sh := &t.shards[shardIndex(id)]
	sh.mu.Lock()
	st := sh.m[id]
	if st != nil {
		delete(sh.m, id)
	}
	sh.mu.Unlock()
	if st != nil {
		atomic.AddInt32(&t.count, -1)
	}
	return st
}

func (t *streamTable) len() int {
	return int(atomic.LoadInt32(&t.count))
}

func (t *streamTable) snapshot() []*Stream {
	out := make([]*Stream, 0, t.len())
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.RLock()
		for _, st := range sh.m {
			out = append(out, st)
		}
		sh.mu.RUnlock()
	}
	return out
}
