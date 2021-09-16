package loadtest

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestComputeSleepDuration(t *testing.T) {
	d := computeSleepDuration(20.0)
	assert.Equal(t, 50*time.Millisecond, d)

	d = computeSleepDuration(1000)
	assert.Equal(t, 1*time.Millisecond, d)
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
		NumRequests:       80,
		RequestsPerSecond: 1000,
		NumThreads:        10,
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
		NumRequests:       80,
		RequestsPerSecond: 1000,
		NumThreads:        10,
		Func: func() {
			atomic.AddUint32(&counter, 1)
		},
		SupplyChanSize: 2048,
	})

	tc.Run()
	tc.Cancel()
}
