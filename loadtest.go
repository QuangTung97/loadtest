package loadtest

import (
	"context"
	"math"
	"sync"
	"time"
)

// Config ...
type Config struct {
	NumRequests       int
	RequestsPerSecond float64
	NumThreads        int
	Func              func()
	SupplyChanSize    int
}

// RunResult ...
type RunResult struct {
	ThreadID  int
	RequestID int
	StartedAt time.Time
	Duration  time.Duration
}

// TestCase ...
type TestCase struct {
	conf          Config
	sleepDuration time.Duration
	supplyChan    chan int
	resultChan    chan RunResult

	ctx       context.Context
	cancel    func()
	wg        sync.WaitGroup
	completed bool
	results   []RunResult
}

func computeSleepDuration(reqsPerSecond float64) time.Duration {
	return time.Duration(math.Round(1000000000/reqsPerSecond)) * time.Nanosecond
}

// New ...
func New(conf Config) *TestCase {
	chanSize := 1024
	if conf.SupplyChanSize > 0 {
		chanSize = conf.SupplyChanSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &TestCase{
		conf:          conf,
		sleepDuration: computeSleepDuration(conf.RequestsPerSecond),
		supplyChan:    make(chan int, chanSize),
		resultChan:    make(chan RunResult, 1024),

		ctx:       ctx,
		cancel:    cancel,
		completed: false,
	}
}

func (tc *TestCase) runConfigFunc(threadID int, requestID int) {
	start := time.Now()
	tc.conf.Func()
	end := time.Now()

	tc.resultChan <- RunResult{
		ThreadID:  threadID,
		RequestID: requestID,
		StartedAt: start,
		Duration:  end.Sub(start),
	}
}

func (tc *TestCase) runThread(threadID int) {
	go func() {
		defer tc.wg.Done()

		for {
			select {
			case <-tc.ctx.Done():
				return
			case requestID, ok := <-tc.supplyChan:
				if !ok {
					return
				}
				tc.runConfigFunc(threadID, requestID)
			}
		}
	}()
}

// Run in background
func (tc *TestCase) Run() {
	tc.wg.Add(tc.conf.NumThreads + 2)

	go func() {
		defer tc.wg.Done()

		for i := 0; i < tc.conf.NumRequests; i++ {
			if tc.ctx.Err() != nil {
				return
			}
			tc.supplyChan <- i
			time.Sleep(tc.sleepDuration)
		}
		close(tc.supplyChan)
	}()

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
func (tc *TestCase) GetRunResults() []RunResult {
	return tc.results
}
