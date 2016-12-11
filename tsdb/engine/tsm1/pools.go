package tsm1

import "sync"

// pool size thresholds
const (
	small  = 524288
	medium = 1048576
	large  = 4194304
)

var (
	bufPoolSmall  sync.Pool
	bufPoolMedium sync.Pool
	bufPoolLarge  sync.Pool

	float64ValuePool sync.Pool
	integerValuePool sync.Pool
	booleanValuePool sync.Pool
	stringValuePool  sync.Pool
)

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) []byte {
	var buf interface{}
	var bucketSize int
	switch {
	case size <= small:
		bucketSize = small
		buf = bufPoolSmall.Get()
	case size <= medium:
		bucketSize = medium
		buf = bufPoolMedium.Get()
	case size <= large:
		bucketSize = large
		buf = bufPoolLarge.Get()
	default:
		return make([]byte, size)
	}

	// pool was empty make a new buffer
	if buf == nil {
		return make([]byte, size, bucketSize)
	}

	return buf.([]byte)[:size]
}

// putBuf returns a buffer to the pool.
func putBuf(buf []byte) {
	size := cap(buf)
	// discard buffers that don't match pool sizes
	switch size {
	case small:
		bufPoolSmall.Put(buf)
	case medium:
		bufPoolMedium.Put(buf)
	case large:
		bufPoolLarge.Put(buf)
	}
}

// getBuf returns a buffer with length size from the buffer pool.
func getFloat64Values(size int) []Value {
	var buf []Value
	x := float64ValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = FloatValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putFloat64Values(buf []Value) {
	float64ValuePool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getIntegerValues(size int) []Value {
	var buf []Value
	x := integerValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = IntegerValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putIntegerValues(buf []Value) {
	integerValuePool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getBooleanValues(size int) []Value {
	var buf []Value
	x := booleanValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = BooleanValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putStringValues(buf []Value) {
	stringValuePool.Put(buf)
}

// getBuf returns a buffer with length size from the buffer pool.
func getStringValues(size int) []Value {
	var buf []Value
	x := stringValuePool.Get()
	if x == nil {
		buf = make([]Value, size)
	} else {
		buf = x.([]Value)
	}
	if cap(buf) < size {
		return make([]Value, size)
	}

	for i, v := range buf {
		if v == nil {
			buf[i] = StringValue{}
		}
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putBooleanValues(buf []Value) {
	booleanValuePool.Put(buf)
}
func putValue(buf []Value) {
	if len(buf) > 0 {
		switch buf[0].(type) {
		case FloatValue:
			putFloat64Values(buf)
		case IntegerValue:
			putIntegerValues(buf)
		case BooleanValue:
			putBooleanValues(buf)
		case StringValue:
			putStringValues(buf)
		}
	}
}
