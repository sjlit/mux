package mux

import (
	"math/bits"
	"sync"
)

const poolBuckets = 17 // 1<<0 .. 1<<16

// bufferPool caches payload buffers in power-of-two size classes so incoming
// frames can be read directly into a buffer that is later handed to a stream
// and recycled once the application consumes it.
type bufferPool struct {
	buckets [poolBuckets]sync.Pool
}

func newBufferPool() *bufferPool {
	p := new(bufferPool)
	for i := 0; i < poolBuckets; i++ {
		size := 1 << uint(i)
		p.buckets[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
	return p
}

// get returns a buffer with len n (n <= 1<<16) and a power-of-two capacity.
func (p *bufferPool) get(n int) *[]byte {
	if n <= 0 {
		return nil
	}
	b := p.buckets[bucketIndex(n)].Get().(*[]byte)
	*b = (*b)[:n]
	return b
}

// put returns a buffer obtained from get. Buffers with an unexpected capacity
// are dropped.
func (p *bufferPool) put(b *[]byte) {
	if b == nil {
		return
	}
	c := cap(*b)
	if c == 0 || c > 1<<16 || c&(c-1) != 0 {
		return
	}
	*b = (*b)[:c]
	p.buckets[bits.TrailingZeros(uint(c))].Put(b)
}

func bucketIndex(n int) int {
	if n <= 1 {
		return 0
	}
	return bits.Len(uint(n - 1))
}
