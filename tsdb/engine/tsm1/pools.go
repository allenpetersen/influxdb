package tsm1

import "sync"

// pool size thresholds
const (
	small  = 524288
	medium = 1048576
	large  = 4194304
)

var (
	bufPoolSmall = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 524288)
		},
	}

	bufPoolMedium = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1048576)
		},
	}

	bufPoolLarge = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4194304)
		},
	}

	float64ValuePool sync.Pool
	integerValuePool sync.Pool
	booleanValuePool sync.Pool
	stringValuePool  sync.Pool
)

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) []byte {
	var result []byte
	switch {
	case size <= small:
		result = bufPoolSmall.Get().([]byte)
	case size <= medium:
		result = bufPoolMedium.Get().([]byte)
	case size <= large:
		result = bufPoolLarge.Get().([]byte)
	default:
		return make([]byte, size)
	}

	return result[:size]
}

// putBuf returns a buffer to the pool.
func putBuf(buf []byte) {
	size := cap(buf)
	switch {
	case size <= small:
		bufPoolSmall.Put(buf)
	case size <= medium:
		bufPoolMedium.Put(buf)
	case size <= large:
		bufPoolLarge.Put(buf)
	default:
		// nothing to do for XLarge buffers
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
