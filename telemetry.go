package main

import (
	"math"
	"sync/atomic"
	"time"
)

// Telemetry tracks performance metrics of the worker pool.
type Telemetry struct {
	Successes       uint64
	Failures        uint64
	BytesRead       uint64
	TotalLat        uint64 // Nanoseconds
	CumulativeBytes uint64 // Tracks total volume safely across threads
}

// Record adds a new result to the telemetry data.
func (t *Telemetry) Record(duration time.Duration, bytes int, err error) {
	if err != nil {
		atomic.AddUint64(&t.Failures, 1)
	} else {
		atomic.AddUint64(&t.Successes, 1)
		atomic.AddUint64(&t.BytesRead, uint64(bytes))
		atomic.AddUint64(&t.TotalLat, uint64(duration.Nanoseconds()))
		atomic.AddUint64(&t.CumulativeBytes, uint64(bytes))
	}
}

// Reset clears the telemetry data for a new generation.
func (t *Telemetry) Reset() {
	atomic.StoreUint64(&t.Successes, 0)
	atomic.StoreUint64(&t.Failures, 0)
	atomic.StoreUint64(&t.BytesRead, 0)
	atomic.StoreUint64(&t.TotalLat, 0)
}

// AtomicPID implements a proportional-integral-derivative controller using atomic operations.
type AtomicPID struct {
	Setpoint int64
	Kp       uint64
}

// Compute calculates the adjustment based on the actual value versus the setpoint.
func (p *AtomicPID) Compute(actual int64) float64 {
	setpoint := atomic.LoadInt64(&p.Setpoint)
	kp := math.Float64frombits(atomic.LoadUint64(&p.Kp))
	return kp * float64(setpoint-actual)
}

// Evaluate calculates a score for a Strategy based on telemetry data.
func Evaluate(t *Telemetry, window time.Duration, s Strategy) float64 {
	succ := atomic.LoadUint64(&t.Successes)
	fail := atomic.LoadUint64(&t.Failures)
	lat := atomic.LoadUint64(&t.TotalLat)
	bytes := atomic.LoadUint64(&t.BytesRead)
	total := succ + fail
	if total == 0 {
		return 0
	}

	mbps := (float64(bytes) / (1024 * 1024)) / window.Seconds()
	avgLatNS := float64(lat / total)

	latFactor := 1.0
	targetNS := float64(s.TargetLatency.Nanoseconds())
	if avgLatNS > targetNS {
		latFactor = math.Exp(-(avgLatNS - targetNS) / targetNS)
	}
	return mbps * latFactor * math.Pow(float64(succ)/float64(total), 2)
}
