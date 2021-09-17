package loadtest

import (
	"errors"
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

func TestValidateConfig(t *testing.T) {
	table := []struct {
		name string
		conf Config
		err  error
	}{
		{
			name: "missing-num-requests",
			conf: Config{},
			err:  errors.New("missing NumRequests"),
		},
		{
			name: "missing-num-threads",
			conf: Config{
				NumRequests: 10,
			},
			err: errors.New("missing NumThreads"),
		},
		{
			name: "missing-func",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
			},
			err: errors.New("missing Func"),
		},
		{
			name: "missing-qps",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS:         QPSConfig{},
			},
			err: errors.New("missing QPS"),
		},
		{
			name: "ok-static",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					StaticValue: 100.0,
				},
			},
			err: nil,
		},
		{
			name: "dynamic-missing-start-value",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic: true,
				},
			},
			err: errors.New("missing QPS.StartValue"),
		},
		{
			name: "dynamic-missing-double-every",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic:  true,
					StartValue: 20.0,
				},
			},
			err: errors.New("missing QPS.DoubleEvery"),
		},
		{
			name: "missing-saturation",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic:   true,
					StartValue:  20.0,
					DoubleEvery: 10 * time.Minute,
				},
			},
			err: errors.New("missing QPS.Saturation"),
		},
		{
			name: "missing-saturation",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic:   true,
					StartValue:  20.0,
					DoubleEvery: 10 * time.Minute,
				},
			},
			err: errors.New("missing QPS.Saturation"),
		},
		{
			name: "missing-qps-consecutive-times",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic:   true,
					StartValue:  20.0,
					DoubleEvery: 10 * time.Minute,
					Saturation: SaturationThreshold{
						BlockedDuration: 2 * time.Millisecond,
					},
				},
			},
			err: errors.New("missing QPS.ConsecutiveTimes"),
		},
		{
			name: "missing-qps-step-back-duration",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic:   true,
					StartValue:  20.0,
					DoubleEvery: 10 * time.Minute,
					Saturation: SaturationThreshold{
						BlockedDuration:  2 * time.Millisecond,
						ConsecutiveTimes: 3,
					},
				},
			},
			err: errors.New("missing QPS.StepBackDuration"),
		},
		{
			name: "ok-dynamic",
			conf: Config{
				NumRequests: 10,
				NumThreads:  3,
				Func:        func() {},
				QPS: QPSConfig{
					IsDynamic:   true,
					StartValue:  20.0,
					DoubleEvery: 10 * time.Minute,
					Saturation: SaturationThreshold{
						BlockedDuration:  2 * time.Millisecond,
						ConsecutiveTimes: 3,
						StepBackDuration: 2 * time.Minute,
					},
				},
			},
			err: nil,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			err := validateConfig(e.conf)
			assert.Equal(t, e.err, err)
		})
	}
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

	startTime := mustParse("2021-09-16T10:00:00+07:00")
	c.start(startTime)

	d := c.getSleepTime(startTime)
	assert.Equal(t, 50*time.Millisecond, d)

	d = c.getSleepTime(startTime.Add(51 * time.Millisecond))
	assert.Equal(t, 48997054*time.Nanosecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:10:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:20:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:20:00+07:00"))
	assert.Equal(t, 12500*time.Microsecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:30:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:30:00+07:00"))
	assert.Equal(t, 6250*time.Microsecond, d)

	d = c.getSleepTime(mustParse("2021-09-16T10:40:00+07:00"))
	assert.Equal(t, 0*time.Microsecond, d)
}

func TestComputeSleepDuration_Saturated(t *testing.T) {
	c := newDynamicQPS(QPSConfig{
		IsDynamic:   true,
		StartValue:  20.0,
		DoubleEvery: 10 * time.Minute,
		Saturation: SaturationThreshold{
			BlockedDuration:  2 * time.Millisecond,
			StepBackDuration: 10 * time.Minute,
			ConsecutiveTimes: 3,
		},
	})

	c.start(mustParse("2021-09-16T10:00:00+07:00"))

	d := c.getSleepTime(mustParse("2021-09-16T10:00:00+07:00"))
	assert.Equal(t, 50*time.Millisecond, d)

	c.blockedDuration(1500 * time.Microsecond)
	c.nextWakeupTime = mustParseNull("2021-09-16T10:10:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.blockedDuration(1500 * time.Microsecond)
	c.blockedDuration(1500 * time.Microsecond)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:10:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.blockedDuration(2 * time.Millisecond)
	c.blockedDuration(2 * time.Millisecond)
	c.blockedDuration(2 * time.Millisecond)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:10:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:10:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:15:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:15:00+07:00"))
	assert.Equal(t, 35355339*time.Nanosecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:20:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:20:00+07:00"))
	assert.Equal(t, 50*time.Millisecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:25:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:25:00+07:00"))
	assert.Equal(t, 35355339*time.Nanosecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:30:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:30:00+07:00"))
	assert.Equal(t, 25*time.Millisecond, d)

	c.nextWakeupTime = mustParseNull("2021-09-16T10:40:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:40:00+07:00"))
	assert.Equal(t, 12500*time.Microsecond, d)

	c.blockedDuration(2 * time.Millisecond)
	c.nextWakeupTime = mustParseNull("2021-09-16T10:50:00+07:00")
	d = c.getSleepTime(mustParse("2021-09-16T10:50:00+07:00"))
	assert.Equal(t, 6250*time.Microsecond, d)

	c.blockedDuration(2 * time.Millisecond)
	c.blockedDuration(2 * time.Millisecond)

	c.nextWakeupTime = mustParseNull("2021-09-16T11:00:00+07:00")
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

	assert.Equal(t, float64(1000), tc.GetRunResults()[0].QPS)
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

func TestRun_WithDynamicQPS(t *testing.T) {
	counter := uint32(0)

	tc := New(Config{
		NumRequests: 80,
		QPS: QPSConfig{
			IsDynamic:   true,
			StartValue:  20,
			DoubleEvery: 50 * time.Millisecond,
			Saturation: SaturationThreshold{
				BlockedDuration:  1 * time.Millisecond,
				ConsecutiveTimes: 2,
				StepBackDuration: 20 * time.Millisecond,
			},
		},
		NumThreads: 10,
		Func: func() {
			atomic.AddUint32(&counter, 1)
		},
		SupplyChanSize: 2048,
	})

	tc.Run()
	tc.WaitFinish()

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
