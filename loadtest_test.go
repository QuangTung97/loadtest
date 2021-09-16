package loadtest

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestComputeSleepDuration_Static(t *testing.T) {
	d := computeSleepDurationStatic(20.0)
	assert.Equal(t, 50*time.Millisecond, d)

	d = computeSleepDurationStatic(1000.0)
	assert.Equal(t, 1*time.Millisecond, d)
}

func TestComputeSleepDuration_Dynamic(t *testing.T) {
	c := newDynamicQPS(QPSConfig{
		IsDynamic:   true,
		StartValue:  20.0,
		DoubleEvery: 10 * time.Minute,
		Saturation: SaturationThreshold{
			BlockedDuration:  2 * time.Millisecond,
			ConsecutiveTimes: 3,
		},
	})

	c.start(mustParse("2021-09-16T10:00:00+07:00"))

	d := c.getSleepTime(mustParse("2021-09-16T10:00:00+07:00"))
	assert.Equal(t, 50*time.Millisecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:20:00+07:00"))
	assert.Equal(t, 12500*time.Microsecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:30:00+07:00"))
	assert.Equal(t, 6250*time.Microsecond, d)
}

func TestComputeSleepDuration_Saturated(t *testing.T) {
	c := newDynamicQPS(QPSConfig{
		IsDynamic:   true,
		StartValue:  20.0,
		DoubleEvery: 10 * time.Minute,
		Saturation: SaturationThreshold{
			BlockedDuration:  2 * time.Millisecond,
			ConsecutiveTimes: 3,
		},
	})

	c.start(mustParse("2021-09-16T10:00:00+07:00"))

	d := c.getSleepTime(mustParse("2021-09-16T10:00:00+07:00"))
	assert.Equal(t, 50*time.Millisecond, d)

	c.blockedDuration(1500 * time.Microsecond)
	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.blockedDuration(1500 * time.Microsecond)
	c.blockedDuration(1500 * time.Microsecond)

	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.blockedDuration(2 * time.Millisecond)
	c.blockedDuration(2 * time.Millisecond)
	c.blockedDuration(2 * time.Millisecond)

	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:15:00+07:00"))
	assert.Equal(t, 35355339*time.Nanosecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:20:00+07:00"))
	assert.Equal(t, 50*time.Millisecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:25:00+07:00"))
	assert.Equal(t, 35355339*time.Nanosecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:30:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:40:00+07:00"))
	assert.Equal(t, 12500*time.Microsecond, d)

	c.blockedDuration(2 * time.Millisecond)
	d = c.getSleepTime(mustParse("2021-09-16T10:50:00+07:00"))
	assert.Equal(t, 6250*time.Microsecond, d)

	c.blockedDuration(2 * time.Millisecond)
	c.blockedDuration(2 * time.Millisecond)

	d = c.getSleepTime(mustParse("2021-09-16T11:00:00+07:00"))
	assert.Equal(t, 12500*time.Microsecond, d)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func TestRun_Completed(t *testing.T) {
	counter := uint32(0)

	tc := New(Config{
		NumRequests: 80,
		QPS: QPSConfig{
			StaticValue: 1000,
		},
		NumThreads: 10,
		Func: func() {
			atomic.AddUint32(&counter, 1)
		},
		SupplyChanSize: 2048,
	})

	tc.Run()
	tc.WaitFinish()
	tc.Cancel()

	assert.Equal(t, uint32(80), counter)
	assert.Equal(t, 80, len(tc.GetRunResults()))

	maxTID := 0
	maxReqID := 0
	for _, r := range tc.GetRunResults() {
		maxTID = maxInt(maxTID, r.ThreadID)
		maxReqID = maxInt(maxReqID, r.RequestID)
	}
	assert.Equal(t, 9, maxTID)
	assert.Equal(t, 79, maxReqID)
}

func TestRun_Cancelled(t *testing.T) {
	counter := uint32(0)

	tc := New(Config{
		NumRequests: 80,
		QPS: QPSConfig{
			StaticValue: 1000,
		},
		NumThreads: 10,
		Func: func() {
			atomic.AddUint32(&counter, 1)
		},
		SupplyChanSize: 2048,
	})

	tc.Run()
	tc.Cancel()
}
