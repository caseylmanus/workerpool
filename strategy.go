package main

import (
	"math/rand"
	"time"
)

// Strategy defines the parameters for the worker pool and its evolutionary search.
type Strategy struct {
	TargetLatency time.Duration // Ideal request cycle time the PID targets
	MaxWorkers    int32         // Maximum concurrent fetchers allowed
	BatchSize     int32         // How many URLs a worker grabs at once
	MinDelay      time.Duration // Base politeness delay between fetches
	Kp            float64       // Proportional gain for the PID backpressure reflex
}

// Clone returns a deep copy of the Strategy.
func (s Strategy) Clone() Strategy {
	return Strategy{
		TargetLatency: s.TargetLatency,
		MaxWorkers:    s.MaxWorkers,
		BatchSize:     s.BatchSize,
		MinDelay:      s.MinDelay,
		Kp:            s.Kp,
	}
}

// Mutate returns a mutated version of the Strategy for the GA evolution.
func Mutate(s Strategy) Strategy {
	m := s.Clone()
	m.TargetLatency += time.Duration(rand.Intn(100)-50) * time.Millisecond
	if m.TargetLatency < 50*time.Millisecond {
		m.TargetLatency = 50 * time.Millisecond
	}
	m.MaxWorkers += int32(rand.Intn(7) - 3)
	if m.MaxWorkers < 1 {
		m.MaxWorkers = 1
	}
	if m.MaxWorkers > 30 {
		m.MaxWorkers = 30
	} // Safety pool limit cap
	m.BatchSize += int32(rand.Intn(3) - 1)
	if m.BatchSize < 1 {
		m.BatchSize = 1
	}
	m.MinDelay += time.Duration(rand.Intn(20)-10) * time.Millisecond
	if m.MinDelay < 0 {
		m.MinDelay = 0
	}
	return m
}
