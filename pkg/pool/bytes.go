package pool

import (
	"fmt"
	"sync/atomic"
)

// Bytes is a pool of byte slices that can be re-used.  Slices in
// this pool will not be garbage collected when not in use.
type Bytes struct {
	pool              chan []byte
	totalPoolCapacity uint64
	getCount          uint64
	putCount          uint64
}

// NewBytes returns a Bytes pool with capacity for max byte slices
// to be pool.
func NewBytes(max int) *Bytes {
	return &Bytes{
		pool: make(chan []byte, max),
	}
}

func (p *Bytes) GetPoolCapacity() uint64 {
	return atomic.LoadUint64(&p.totalPoolCapacity)
}

func (p *Bytes) GetCounts() (uint64, uint64) {
	return atomic.LoadUint64(&p.getCount), atomic.LoadUint64(&p.putCount)
}

// Get returns a byte slice size with at least sz capacity. Items
// returned may not be in the zero state and should be reset by the
// caller.
func (p *Bytes) Get(size, poolSize int) []byte {
	atomic.AddUint64(&p.getCount, 1)
	var c []byte
	select {
	case c = <-p.pool:
		atomic.AddUint64(&p.totalPoolCapacity, uint64(cap(c)*-1))
	default:
		fmt.Printf("buff pool empty wanted %9d\n", size)
		return make([]byte, size, poolSize)
	}

	if cap(c) < size {
		fmt.Printf("buff too small, wanted %9d got %9d\n", size, cap(c))
		return make([]byte, size, poolSize)
	}

	return c[:size]
}

// Put returns a slice back to the pool.  If the pool is full, the byte
// slice is discarded.
func (p *Bytes) Put(c []byte) {
	atomic.AddUint64(&p.putCount, 1)
	cCap := cap(c)
	select {
	case p.pool <- c:
		atomic.AddUint64(&p.totalPoolCapacity, uint64(cCap))
	default:
	}
}
