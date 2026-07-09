package main

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// 1. SYSTEM GENOME & CONFIGURATION
// ============================================================================

type Strategy struct {
	TargetLatency time.Duration // Ideal request cycle time the PID targets
	MaxWorkers    int32         // Maximum concurrent fetchers allowed
	BatchSize     int32         // How many URLs a worker processes per loop iteration
	MinDelay      time.Duration // Base politeness delay between fetches
	Kp            float64       // Proportional gain for the PID backpressure reflex
}

func (s Strategy) Clone() Strategy {
	return Strategy{
		TargetLatency: s.TargetLatency,
		MaxWorkers:    s.MaxWorkers,
		BatchSize:     s.BatchSize,
		MinDelay:      s.MinDelay,
		Kp:            s.Kp,
	}
}

// ============================================================================
// 2. CONCURRENT DATA STRUCTURES (Queue & Buffers)
// ============================================================================

type URLQueue struct {
	mu    sync.Mutex
	links []string
	seen  map[string]bool
}

func NewURLQueue() *URLQueue {
	return &URLQueue{
		links: make([]string, 0),
		seen:  make(map[string]bool),
	}
}

func (q *URLQueue) Push(urls []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, u := range urls {
		if !q.seen[u] {
			q.seen[u] = true
			q.links = append(q.links, u)
		}
	}
}

func (q *URLQueue) PopBatch(batchSize int) []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.links) == 0 {
		return nil
	}

	size := batchSize
	if len(q.links) < size {
		size = len(q.links)
	}

	batch := q.links[:size]
	q.links = q.links[size:]
	return batch
}

// ============================================================================
// 3. TELEMETRY & STRATEGIC LAYERS
// ============================================================================

type Telemetry struct {
	Successes uint64
	Failures  uint64
	BytesRead uint64
	TotalLat  uint64 // Nanoseconds
}

func (t *Telemetry) Record(duration time.Duration, bytes int, err error) {
	if err != nil {
		atomic.AddUint64(&t.Failures, 1)
	} else {
		atomic.AddUint64(&t.Successes, 1)
		atomic.AddUint64(&t.BytesRead, uint64(bytes))
		atomic.AddUint64(&t.TotalLat, uint64(duration.Nanoseconds()))
	}
}

func (t *Telemetry) Reset() {
	atomic.StoreUint64(&t.Successes, 0)
	atomic.StoreUint64(&t.Failures, 0)
	atomic.StoreUint64(&t.BytesRead, 0)
	atomic.StoreUint64(&t.TotalLat, 0)
}

type AtomicPID struct {
	setpoint int64
	kp       uint64
}

func (p *AtomicPID) Compute(actual int64) float64 {
	setpoint := atomic.LoadInt64(&p.setpoint)
	kp := math.Float64frombits(atomic.LoadUint64(&p.kp))

	err := setpoint - actual // Positive = too fast, Negative = too slow (high latency)
	return kp * float64(err)
}

func Evaluate(t *Telemetry, window time.Duration, s Strategy) float64 {
	succ := atomic.LoadUint64(&t.Successes)
	fail := atomic.LoadUint64(&t.Failures)
	lat := atomic.LoadUint64(&t.TotalLat)
	bytes := atomic.LoadUint64(&t.BytesRead)
	total := succ + fail

	if total == 0 { return 0 }

	// Throughput metrics: We want raw corpus ingestion volume (MB/sec)
	mbps := (float64(bytes) / (1024 * 1024)) / window.Seconds()
	avgLatNS := float64(lat / total)

	// Penalize strategies that trigger Wikipedia rate limits or drops
	latFactor := 1.0
	targetNS := float64(s.TargetLatency.Nanoseconds())
	if avgLatNS > targetNS {
		latFactor = math.Exp(-(avgLatNS - targetNS) / targetNS)
	}

	reliability := math.Pow(float64(succ)/float64(total), 2)
	return mbps * latFactor * reliability
}

func Mutate(s Strategy) Strategy {
	mutated := s.Clone()

	mutated.TargetLatency += time.Duration(rand.Intn(100)-50) * time.Millisecond
	if mutated.TargetLatency < 50*time.Millisecond { mutated.TargetLatency = 50 * time.Millisecond }

	mutated.MaxWorkers += int32(rand.Intn(7) - 3)
	if mutated.MaxWorkers < 1 { mutated.MaxWorkers = 1 }
	if mutated.MaxWorkers > 50 { mutated.MaxWorkers = 50 } // Infrastructure safety cap

	mutated.BatchSize += int32(rand.Intn(3) - 1)
	if mutated.BatchSize < 1 { mutated.BatchSize = 1 }

	mutated.MinDelay += time.Duration(rand.Intn(20)-10) * time.Millisecond
	if mutated.MinDelay < 0 { mutated.MinDelay = 0 }

	return mutated
}

// ============================================================================
// 4. PARSING & NETWORK ENGINE
// ============================================================================

var wikiLinkRegex = regexp.MustCompile(`href="/wiki/([^:#"]+)"`)

func CrawlPage(url string) ([]string, int, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	// Extract links for next crawler generations
	matches := wikiLinkRegex.FindAllStringSubmatch(string(bodyBytes), -1)
	discovered := make([]string, 0)
	for _, match := range matches {
		if len(match) > 1 && !strings.Contains(match[1], "Wikipedia") {
			discovered = append(discovered, "https://en.wikipedia.org/wiki/"+match[1])
		}
	}

	return discovered, len(bodyBytes), nil
}

// ============================================================================
// 5. RUNTIME ORCHESTRATION
// ============================================================================

func main() {
	queue := NewURLQueue()
	telemetry := &Telemetry{}
	genWindow := 10 * time.Second

	// Seed links to start the process
	queue.Push([]string{
		"https://en.wikipedia.org/wiki/Artificial_intelligence",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://en.wikipedia.org/wiki/Control_theory",
	})

	currentStrategy := Strategy{
		TargetLatency: 300 * time.Millisecond,
		MaxWorkers:    5,
		BatchSize:     2,
		MinDelay:      20 * time.Millisecond,
		Kp:            0.2,
	}

	pid := &AtomicPID{
		setpoint: int64(currentStrategy.TargetLatency),
		kp:       math.Float64bits(currentStrategy.Kp),
	}

	var activeWorkers int32 = 0
	var currentBatch int32 = currentStrategy.BatchSize
	var currentMinDelay int64 = int64(currentStrategy.MinDelay)

	adjustWorkers := func(target int32) {
		for {
			current := atomic.LoadInt32(&activeWorkers)
			if current == target { break }
			if current < target {
				if atomic.CompareAndSwapInt32(&activeWorkers, current, current+1) {
					go func(workerID int) {
						pidThrottle := time.Duration(0)
						for {
							if int32(workerID) >= atomic.LoadInt32(&activeWorkers) {
								atomic.AddInt32(&activeWorkers, -1)
								return
							}

							batchSize := int(atomic.LoadInt32(&currentBatch))
							urls := queue.PopBatch(batchSize)

							if len(urls) == 0 {
								time.Sleep(200 * time.Millisecond) // Queue starvation cooling
								continue
							}

							for _, url := range urls {
								start := time.Now()
								links, bytes, err := CrawlPage(url)
								duration := time.Since(start)

								telemetry.Record(duration, bytes, err)

								if err == nil && len(links) > 0 {
									queue.Push(links)
								}

								// Execute Tactical Loop Adjustment
								adjust := pid.Compute(int64(duration))
								if adjust < 0 {
									// We are breaking latency thresholds; increase structural pacing delay
									pidThrottle = time.Duration(math.Abs(adjust)/1e6) * time.Millisecond
									if pidThrottle > 2*time.Second { pidThrottle = 2 * time.Second }
								} else {
									pidThrottle = 0
								}

								baseDelay := time.Duration(atomic.LoadInt64(&currentMinDelay))
								time.Sleep(baseDelay + pidThrottle)
							}
						}
					}(int(current))
				}
			} else {
				atomic.StoreInt32(&activeWorkers, target)
				break
			}
		}
	}

	adjustWorkers(currentStrategy.MaxWorkers)

	// ASYNCHRONOUS STRATEGIC SYSTEM MONITOR (The GA)
	go func() {
		for {
			time.Sleep(genWindow)

			score := Evaluate(telemetry, genWindow, currentStrategy)
			succ := atomic.LoadUint64(&telemetry.Successes)
			bytes := atomic.LoadUint64(&telemetry.BytesRead)

			fmt.Printf("\n[GA GEN EVAL] Fitness Score: %6.2f | Ingested: %.2f MB | Requests: %d | Workers: %d\n",
				score, float64(bytes)/(1024*1024), succ, currentStrategy.MaxWorkers)

			// Simple evolutionary transition strategy
			nextStrategy := Mutate(currentStrategy)

			// Dynamic hot swap
			currentStrategy = nextStrategy
			atomic.StoreInt32(&currentBatch, currentStrategy.BatchSize)
			atomic.StoreInt64(&currentMinDelay, int64(currentStrategy.MinDelay))
			atomic.StoreInt64(&pid.setpoint, int64(currentStrategy.TargetLatency))
			atomic.StoreUint64(&pid.kp, math.Float64bits(currentStrategy.Kp))

			adjustWorkers(currentStrategy.MaxWorkers)
			telemetry.Reset()
		}
	}()

	select {}
}
