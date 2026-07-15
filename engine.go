package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

// Engine manages the worker pool, telemetry, and evolutionary search.
type Engine struct {
	Queue           *URLQueue
	Telemetry       *Telemetry
	PID             *AtomicPID
	CurrentStrategy Strategy

	ActiveWorkers    int32
	WorkerIDSequence int32
	CurrentBatch     int32
	CurrentMinDelay  int64
	UIMode           string

	GenWindow time.Duration
}

// NewEngine creates a new Engine with default values.
func NewEngine() *Engine {
	strategy := Strategy{
		TargetLatency: 400 * time.Millisecond,
		MaxWorkers:    4,
		BatchSize:     1,
		MinDelay:      100 * time.Millisecond,
		Kp:            0.2,
	}
	return &Engine{
		Queue:           NewURLQueue(),
		Telemetry:       &Telemetry{},
		PID:             &AtomicPID{Setpoint: int64(strategy.TargetLatency), Kp: math.Float64bits(strategy.Kp)},
		CurrentStrategy: strategy,
		CurrentBatch:    strategy.BatchSize,
		CurrentMinDelay: int64(strategy.MinDelay),
		UIMode:          "INITIALIZING",
		GenWindow:       5 * time.Second,
	}
}

// Start begins the background processes for the engine.
func (e *Engine) Start() {
	e.Queue.Push([]string{
		"https://en.wikipedia.org/wiki/Artificial_intelligence",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://en.wikipedia.org/wiki/Control_theory",
	})

	go e.seederLoop()
	e.AdjustWorkers(e.CurrentStrategy.MaxWorkers)
	go e.evolutionLoop()
}

func (e *Engine) seederLoop() {
	for {
		if e.Queue.Len() < 5 {
			e.Queue.Push([]string{fmt.Sprintf("https://en.wikipedia.org/wiki/Special:Random?r=%d", rand.Intn(100000))})
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// AdjustWorkers changes the number of active workers to match the target.
func (e *Engine) AdjustWorkers(target int32) {
	for {
		current := atomic.LoadInt32(&e.ActiveWorkers)
		if current == target {
			break
		}
		if current < target {
			if atomic.CompareAndSwapInt32(&e.ActiveWorkers, current, current+1) {
				id := atomic.AddInt32(&e.WorkerIDSequence, 1) - 1
				go e.worker(id)
			}
		} else {
			_ = atomic.CompareAndSwapInt32(&e.ActiveWorkers, current, current-1)
		}
	}
}

func (e *Engine) worker(myID int32) {
	pidThrottle := time.Duration(0)
	for {
		if myID >= atomic.LoadInt32(&e.ActiveWorkers) {
			return
		}
		batchSize := int(atomic.LoadInt32(&e.CurrentBatch))
		urls := e.Queue.PopBatch(batchSize)
		if len(urls) == 0 {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		for _, url := range urls {
			if myID >= atomic.LoadInt32(&e.ActiveWorkers) {
				return
			}

			duration := e.processURL(url)
			pidThrottle = e.computePIDThrottle(duration)

			time.Sleep(time.Duration(atomic.LoadInt64(&e.CurrentMinDelay)) + pidThrottle)
		}
	}
}

func (e *Engine) processURL(url string) time.Duration {
	start := time.Now()
	links, bytes, err := CrawlPage(url)
	duration := time.Since(start)
	e.Telemetry.Record(duration, bytes, err)

	if err == nil && len(links) > 0 {
		e.Queue.Push(links)
	}
	return duration
}

func (e *Engine) computePIDThrottle(duration time.Duration) time.Duration {
	adjust := e.PID.Compute(int64(duration))
	if adjust < 0 {
		pidThrottle := time.Duration(math.Abs(adjust)/1e6) * time.Millisecond
		if pidThrottle > 2*time.Second {
			pidThrottle = 2 * time.Second
		}
		return pidThrottle
	}
	return 0
}

func (e *Engine) evolutionLoop() {
	recoveryMode := false
	bestScore := 0.0
	bestStrategy := e.CurrentStrategy.Clone()
	e.UIMode = "DARWINIAN SEARCH"

	for {
		time.Sleep(e.GenWindow)
		score := Evaluate(e.Telemetry, e.GenWindow, e.CurrentStrategy)
		succ := atomic.LoadUint64(&e.Telemetry.Successes)
		fail := atomic.LoadUint64(&e.Telemetry.Failures)
		total := succ + fail

		// Elitism check
		if score > bestScore && fail == 0 && total > 0 {
			bestScore = score
			bestStrategy = e.CurrentStrategy.Clone()
		}

		var nextStrategy Strategy
		nextStrategy, recoveryMode = e.selectNextStrategy(total, fail, recoveryMode, bestStrategy)

		e.updateStrategy(nextStrategy)
		e.Telemetry.Reset()
	}
}

func (e *Engine) selectNextStrategy(total, fail uint64, recoveryMode bool, bestStrategy Strategy) (Strategy, bool) {
	if total > 0 && float64(fail)/float64(total) > 0.50 {
		e.UIMode = "PANIC / SCALE BACK"
		nextStrategy := e.CurrentStrategy.Clone()
		nextStrategy.MaxWorkers = 1
		nextStrategy.BatchSize = 1
		nextStrategy.MinDelay = 2 * time.Second
		return nextStrategy, true
	}

	if total == 0 {
		e.UIMode = "QUEUE STARVATION"
		nextStrategy := e.CurrentStrategy.Clone()
		nextStrategy.MaxWorkers = 2
		return nextStrategy, recoveryMode
	}

	if recoveryMode {
		e.UIMode = "RECOVERY PROBE"
		nextStrategy := bestStrategy.Clone()
		nextStrategy.MaxWorkers = atomic.LoadInt32(&e.ActiveWorkers) + 1
		if nextStrategy.MaxWorkers >= 4 {
			return nextStrategy, false
		}
		return nextStrategy, true
	}

	e.UIMode = "DARWINIAN SEARCH"
	if rand.Float64() < 0.75 {
		return Mutate(bestStrategy), false
	}
	return bestStrategy.Clone(), false
}

func (e *Engine) updateStrategy(s Strategy) {
	e.CurrentStrategy = s
	atomic.StoreInt32(&e.CurrentBatch, e.CurrentStrategy.BatchSize)
	atomic.StoreInt64(&e.CurrentMinDelay, int64(e.CurrentStrategy.MinDelay))
	atomic.StoreInt64(&e.PID.Setpoint, int64(e.CurrentStrategy.TargetLatency))
	atomic.StoreUint64(&e.PID.Kp, math.Float64bits(e.CurrentStrategy.Kp))
	e.AdjustWorkers(e.CurrentStrategy.MaxWorkers)
}
