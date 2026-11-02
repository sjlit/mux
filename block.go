package mux

import (
	"sync"
	"sync/atomic"
)

// frameOwner releases the backing storage of one received PSH payload. It is
// stored by value in the stream ring so the common path does not allocate an
// interface wrapper.
type frameOwner struct {
	block *frameBlock
	buf   *[]byte
	pool  *bufferPool
}

func (o frameOwner) release() {
	if o.block != nil {
		o.block.release()
	} else if o.buf != nil && o.pool != nil {
		o.pool.put(o.buf)
	}
}

// frameBlock is a batched receive buffer. The reader holds one reference while
// parsing; every queued PSH payload adds one. The block is recycled when the
// last consumer releases it.
type frameBlock struct {
	data []byte
	refs atomic.Int32
	pool *frameBlockPool
}

func (b *frameBlock) retain() {
	b.refs.Add(1)
}

func (b *frameBlock) release() {
	if b.refs.Add(-1) == 0 {
		b.pool.pool.Put(b)
	}
}

type frameBlockPool struct {
	size   int
	allocs atomic.Int64
	pool   sync.Pool
}

func newFrameBlockPool(size int) *frameBlockPool {
	p := &frameBlockPool{size: size}
	p.pool.New = func() any {
		p.allocs.Add(1)
		return &frameBlock{
			data: make([]byte, size),
			pool: p,
		}
	}
	return p
}

func (p *frameBlockPool) get() *frameBlock {
	b := p.pool.Get().(*frameBlock)
	b.refs.Store(1)
	return b
}

// blockPools keeps a small block for latency/small-frame transports and a
// large block for full-size frames. Large frames grow the current block on
// demand, then the reader falls back to a small block for subsequent frames.
type blockPools struct {
	small *frameBlockPool
	large *frameBlockPool
}

func newBlockPools(smallSize, largeSize int) *blockPools {
	if smallSize < 64 {
		smallSize = 64
	}
	if largeSize < smallSize {
		largeSize = smallSize
	}
	return &blockPools{
		small: newFrameBlockPool(smallSize),
		large: newFrameBlockPool(largeSize),
	}
}
