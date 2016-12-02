package tsm1

import (
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/pool"
)

var (
	bufPoolSmall     = pool.NewBytes(256)
	bufPoolMedium    = pool.NewBytes(256)
	bufPoolLarge     = pool.NewBytes(256)
	bufPoolXLarge    = pool.NewBytes(16)
	float64ValuePool sync.Pool
	integerValuePool sync.Pool
	booleanValuePool sync.Pool
	stringValuePool  sync.Pool
)

func init() {
	// output pool stats to stdout
	go func() {
		for {
			capS := bufPoolSmall.GetPoolCapacity() / 1048576
			getS, putS := bufPoolSmall.GetCounts()

			capM := bufPoolMedium.GetPoolCapacity() / 1048576
			getM, putM := bufPoolMedium.GetCounts()

			capL := bufPoolLarge.GetPoolCapacity() / 1048576
			getL, putL := bufPoolLarge.GetCounts()

			capXL := bufPoolXLarge.GetPoolCapacity() / 1048576
			getXL, putXL := bufPoolXLarge.GetCounts()

			fmt.Printf("%s bufPool capacities s: %d (%6d %6d) m: %d (%6d %6d) l: %d (%6d %6d) xl: %d (%6d %6d) total: %d\n",
				time.Now().Format("2006-01-02 15:04:05"),
				capS, getS, putS,
				capM, getM, putM,
				capL, getL, putL,
				capXL, getXL, putXL,
				capS+capM+capL+capXL,
			)
			time.Sleep(5 * time.Second)
		}
	}()

}

// pool size thresholds
const (
	small  = 524288
	medium = 1048576
	large  = 4194304
)

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) []byte {
	switch {
	case size <= small:
		return bufPoolSmall.Get(size, small)
	case size <= medium:
		return bufPoolMedium.Get(size, medium)
	case size <= large:
		return bufPoolLarge.Get(size, large)
	default:
		return bufPoolXLarge.Get(size, size)
	}
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
		bufPoolXLarge.Put(buf)
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
