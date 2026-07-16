package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Engine manages the worker pool, telemetry, and evolutionary search.
type Engine struct {
	Queue           *URLQueue
	Telemetry       *Telemetry
	PID             *AtomicPID
	mu              sync.RWMutex
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

// GetStrategy returns a copy of the current strategy.
func (e *Engine) GetStrategy() Strategy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.CurrentStrategy
}

// GetUIMode returns the current UI mode string.
func (e *Engine) GetUIMode() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.UIMode
}

// SetUIMode updates the UI mode string.
func (e *Engine) SetUIMode(mode string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.UIMode = mode
}

// Start begins the background processes for the engine.
func (e *Engine) Start() {
	e.Queue.Push([]string{
		"https://en.wikipedia.org/wiki/Artificial_intelligence",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://en.wikipedia.org/wiki/Control_theory",
	})

	go e.seederLoop()
	e.AdjustWorkers(e.GetStrategy().MaxWorkers)
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
	bestStrategy := e.GetStrategy().Clone()
	e.SetUIMode("DARWINIAN SEARCH")

	for {
		time.Sleep(e.GenWindow)
		strategy := e.GetStrategy()
		score := Evaluate(e.Telemetry, e.GenWindow, strategy)
		succ := atomic.LoadUint64(&e.Telemetry.Successes)
		fail := atomic.LoadUint64(&e.Telemetry.Failures)
		total := succ + fail

		// Elitism check
		if score > bestScore && fail == 0 && total > 0 {
			bestScore = score
			bestStrategy = strategy.Clone()
		}

		var nextStrategy Strategy
		nextStrategy, recoveryMode = e.selectNextStrategy(total, fail, recoveryMode, bestStrategy)

		e.updateStrategy(nextStrategy)
		e.Telemetry.Reset()
	}
}

func (e *Engine) selectNextStrategy(total, fail uint64, recoveryMode bool, bestStrategy Strategy) (Strategy, bool) {
	strategy := e.GetStrategy()
	if total > 0 && float64(fail)/float64(total) > 0.50 {
		e.SetUIMode("PANIC / SCALE BACK")
		nextStrategy := strategy.Clone()
		nextStrategy.MaxWorkers = 1
		nextStrategy.BatchSize = 1
		nextStrategy.MinDelay = 2 * time.Second
		return nextStrategy, true
	}

	if total == 0 {
		e.SetUIMode("QUEUE STARVATION")
		nextStrategy := strategy.Clone()
		nextStrategy.MaxWorkers = 2
		return nextStrategy, recoveryMode
	}

	if recoveryMode {
		e.SetUIMode("RECOVERY PROBE")
		nextStrategy := bestStrategy.Clone()
		nextStrategy.MaxWorkers = atomic.LoadInt32(&e.ActiveWorkers) + 1
		if nextStrategy.MaxWorkers >= 4 {
			return nextStrategy, false
		}
		return nextStrategy, true
	}

	e.SetUIMode("DARWINIAN SEARCH")
	if rand.Float64() < 0.75 {
		return Mutate(bestStrategy), false
	}
	return bestStrategy.Clone(), false
}

func (e *Engine) updateStrategy(s Strategy) {
	e.mu.Lock()
	e.CurrentStrategy = s
	e.mu.Unlock()
	atomic.StoreInt32(&e.CurrentBatch, s.BatchSize)
	atomic.StoreInt64(&e.CurrentMinDelay, int64(s.MinDelay))
	atomic.StoreInt64(&e.PID.Setpoint, int64(s.TargetLatency))
	atomic.StoreUint64(&e.PID.Kp, math.Float64bits(s.Kp))
	e.AdjustWorkers(s.MaxWorkers)
}

