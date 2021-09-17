package loadtest

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sync"
	"time"
)

// SaturationThreshold ...
type SaturationThreshold struct {
	BlockedDuration  time.Duration
	StepBackDuration time.Duration
	ConsecutiveTimes int
}

// QPSConfig ...
type QPSConfig struct {
	StaticValue float64
	IsDynamic   bool
	StartValue  float64
	DoubleEvery time.Duration
	Saturation  SaturationThreshold
}

// Config ...
type Config struct {
	NumRequests    int
	QPS            QPSConfig
	NumThreads     int
	Func           func()
	SupplyChanSize int
}

// RunResult ...
type RunResult struct {
	ThreadID        int
	RequestID       int
	StartedAt       time.Time
	Duration        time.Duration
	QPS             float64
	BlockedDuration time.Duration
}

type supplyData struct {
	reqID           int
	qps             float64
	blockedDuration time.Duration
}

// TestCase ...
type TestCase struct {
	conf       Config
	supplyChan chan supplyData
	resultChan chan RunResult

	ctx       context.Context
	cancel    func()
	wg        sync.WaitGroup
	completed bool
	results   []RunResult
}

func computeSleepDurationStatic(qps float64) time.Duration {
	return time.Duration(math.Round(1000000000/qps)) * time.Nanosecond
}

func validateConfigForDynamicQPS(qps QPSConfig) error {
	if qps.StartValue <= 0 {
		return errors.New("missing QPS.StartValue")
	}
	if qps.DoubleEvery <= 0 {
		return errors.New("missing QPS.DoubleEvery")
	}
	saturation := qps.Saturation
	if saturation.BlockedDuration <= 0 {
		return errors.New("missing QPS.Saturation")
	}
	if saturation.ConsecutiveTimes <= 0 {
		return errors.New("missing QPS.ConsecutiveTimes")
	}
	if saturation.StepBackDuration <= 0 {
		return errors.New("missing QPS.StepBackDuration")
	}

	return nil
}

func validateConfig(conf Config) error {
	if conf.NumRequests <= 0 {
		return errors.New("missing NumRequests")
	}
	if conf.NumThreads <= 0 {
		return errors.New("missing NumThreads")
	}
	if conf.Func == nil {
		return errors.New("missing Func")
	}
	if !conf.QPS.IsDynamic && conf.QPS.StaticValue <= 0.0 {
		return errors.New("missing QPS")
	}
	qps := conf.QPS
	if qps.IsDynamic {
		err := validateConfigForDynamicQPS(qps)
		if err != nil {
			return err
		}
	}
	return nil
}

type dynamicQPS struct {
	conf QPSConfig

	startValue float64
	startTime  time.Time

	lastValue      float64
	lastTime       time.Time
	nextWakeupTime sql.NullTime

	blockedCount int
	goDown       bool
}

func newDynamicQPS(conf QPSConfig) *dynamicQPS {
	return &dynamicQPS{
		conf:         conf,
		startValue:   conf.StartValue,
		blockedCount: 0,
	}
}

func (d *dynamicQPS) start(now time.Time) {
	d.startTime = now
}

func (d *dynamicQPS) getSleepTime(now time.Time) time.Duration {
	d.lastTime = now

	diff := now.Sub(d.startTime)
	n := float64(diff) / float64(d.conf.DoubleEvery)

	if d.goDown {
		n = -n
	}

	k := math.Pow(2.0, n)
	d.lastValue = d.startValue * k
	result := 1000000000 / d.lastValue

	if d.goDown && diff >= d.conf.Saturation.StepBackDuration {
		d.goDown = false
		d.setStartValues()
	}

	duration := time.Duration(math.Round(result)) * time.Nanosecond
	if d.nextWakeupTime.Valid {
		duration -= now.Sub(d.nextWakeupTime.Time)
		if duration <= 0 {
			duration = 0
		}
	}
	d.nextWakeupTime = sql.NullTime{
		Valid: true,
		Time:  now.Add(duration),
	}
	return duration
}

func (d *dynamicQPS) setStartValues() {
	d.startTime = d.lastTime
	d.startValue = d.lastValue
	d.blockedCount = 0
}

func (d *dynamicQPS) blockedDuration(duration time.Duration) {
	if duration < d.conf.Saturation.BlockedDuration {
		d.blockedCount = 0
		return
	}
	d.blockedCount++

	if d.blockedCount >= d.conf.Saturation.ConsecutiveTimes {
		d.goDown = true
		d.setStartValues()
	}
}

// New ...
func New(conf Config) *TestCase {
	if err := validateConfig(conf); err != nil {
		panic(err)
	}

	chanSize := 10
	if conf.SupplyChanSize > 0 {
		chanSize = conf.SupplyChanSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &TestCase{
		conf:       conf,
		supplyChan: make(chan supplyData, chanSize),
		resultChan: make(chan RunResult, 1024),

		ctx:       ctx,
		cancel:    cancel,
		completed: false,
	}
}

func (tc *TestCase) runConfigFunc(threadID int, data supplyData) {
	start := time.Now()
	tc.conf.Func()
	end := time.Now()

	tc.resultChan <- RunResult{
		ThreadID:        threadID,
		RequestID:       data.reqID,
		StartedAt:       start,
		Duration:        end.Sub(start),
		QPS:             data.qps,
		BlockedDuration: data.blockedDuration,
	}
}

func (tc *TestCase) runThread(threadID int) {
	go func() {
		defer tc.wg.Done()

		for {
			select {
			case <-tc.ctx.Done():
				return
			case data, ok := <-tc.supplyChan:
				if !ok {
					return
				}
				tc.runConfigFunc(threadID, data)
			}
		}
	}()
}

func (tc *TestCase) supplyWithStaticQPS() {
	defer tc.wg.Done()

	lastBlocked := time.Duration(0)
	for i := 0; i < tc.conf.NumRequests; i++ {
		if tc.ctx.Err() != nil {
			return
		}

		begin := time.Now()
		tc.supplyChan <- supplyData{
			reqID:           i,
			qps:             tc.conf.QPS.StaticValue,
			blockedDuration: lastBlocked,
		}
		end := time.Now()
		lastBlocked = end.Sub(begin)

		time.Sleep(computeSleepDurationStatic(tc.conf.QPS.StaticValue))
	}
	close(tc.supplyChan)
}

func (tc *TestCase) supplyWithDynamicQPS() {
	defer tc.wg.Done()

	d := newDynamicQPS(tc.conf.QPS)
	d.start(time.Now())

	lastBlocked := time.Duration(0)
	for i := 0; i < tc.conf.NumRequests; i++ {
		if tc.ctx.Err() != nil {
			return
		}

		begin := time.Now()
		tc.supplyChan <- supplyData{
			reqID:           i,
			qps:             d.lastValue,
			blockedDuration: lastBlocked,
		}
		end := time.Now()
		lastBlocked = end.Sub(begin)

		d.blockedDuration(end.Sub(begin))
		time.Sleep(d.getSleepTime(end))
	}
	close(tc.supplyChan)
}

// Run in background
func (tc *TestCase) Run() {
	tc.wg.Add(tc.conf.NumThreads + 2)

	if tc.conf.QPS.IsDynamic {
		go tc.supplyWithDynamicQPS()
	} else {
		go tc.supplyWithStaticQPS()
	}

	go func() {
		defer tc.wg.Done()

		for i := 0; i < tc.conf.NumRequests; i++ {
			select {
			case <-tc.ctx.Done():
				return
			case result := <-tc.resultChan:
				tc.results = append(tc.results, result)
			}
		}
	}()

	for i := 0; i < tc.conf.NumThreads; i++ {
		tc.runThread(i)
	}
}

// Cancel ...
func (tc *TestCase) Cancel() {
	if !tc.completed {
		tc.completed = true
		tc.cancel()
		tc.wg.Wait()
	}
}

// WaitFinish ...
func (tc *TestCase) WaitFinish() {
	if !tc.completed {
		tc.completed = true
		tc.wg.Wait()
	}
}

// GetRunResults ...
func (tc *TestCase) GetRunResults() RunResultList {
	return tc.results
}
