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
	BatchSize     int32         // How many URLs a worker grabs at once
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

	if total == 0 {
		return 0
	}

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
	if mutated.TargetLatency < 50*time.Millisecond {
		mutated.TargetLatency = 50 * time.Millisecond
	}

	mutated.MaxWorkers += int32(rand.Intn(7) - 3)
	if mutated.MaxWorkers < 1 {
		mutated.MaxWorkers = 1
	}
	if mutated.MaxWorkers > 40 {
		mutated.MaxWorkers = 40
	} // Infrastructure safety cap

	mutated.BatchSize += int32(rand.Intn(3) - 1)
	if mutated.BatchSize < 1 {
		mutated.BatchSize = 1
	}

	mutated.MinDelay += time.Duration(rand.Intn(20)-10) * time.Millisecond
	if mutated.MinDelay < 0 {
		mutated.MinDelay = 0
	}

	return mutated
}

// ============================================================================
// 4. PARSING & NETWORK ENGINE
// ============================================================================

var wikiLinkRegex = regexp.MustCompile(`href="/wiki/([^:#\"'<>]+)"`)

func CrawlPage(url string) ([]string, int, error) {
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, 0, err
	}

	// Wikipedia demands a unique User-Agent identifier to prevent blocking
	req.Header.Set("User-Agent", "GopherConDemoBot/1.0 (contact@example.com) Go-LLM-Project")

	resp, err := client.Do(req)
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
// 5. RUNTIME ORCHESTRATION WITH DETERMINISTIC CONTROL PLANE
// ============================================================================

func main() {
	rand.Seed(time.Now().UnixNano())
	queue := NewURLQueue()
	telemetry := &Telemetry{}
	genWindow := 5 * time.Second

	// Seed links to start the engine
	queue.Push([]string{
		"https://en.wikipedia.org/wiki/Artificial_intelligence",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://en.wikipedia.org/wiki/Control_theory",
	})

	// BACKGROUND SEEDER: Keeps the queue alive without worker lock contention
	go func() {
		for {
			queue.mu.Lock()
			qLen := len(queue.links)
			queue.mu.Unlock()

			if qLen < 5 {
				queue.Push([]string{
					fmt.Sprintf("https://en.wikipedia.org/wiki/Special:Random?r=%d", rand.Intn(100000)),
				})
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	currentStrategy := Strategy{
		TargetLatency: 400 * time.Millisecond,
		MaxWorkers:    4,
		BatchSize:     1,
		MinDelay:      100 * time.Millisecond,
		Kp:            0.2,
	}

	pid := &AtomicPID{
		setpoint: int64(currentStrategy.TargetLatency),
		kp:       math.Float64bits(currentStrategy.Kp),
	}

	var activeWorkers int32 = 0
	var workerIDSequence int32 = 0
	var currentBatch int32 = currentStrategy.BatchSize
	var currentMinDelay int64 = int64(currentStrategy.MinDelay)

	// Fixed Thread-Safe Worker Scaling Mechanism (Token/Sequence Based)
	adjustWorkers := func(target int32) {
		for {
			current := atomic.LoadInt32(&activeWorkers)
			if current == target {
				break
			}

			if current < target {
				// Scale Up path
				if atomic.CompareAndSwapInt32(&activeWorkers, current, current+1) {
					id := atomic.AddInt32(&workerIDSequence, 1) - 1

					go func(myID int32) {
						pidThrottle := time.Duration(0)
						for {
							if myID >= atomic.LoadInt32(&activeWorkers) {
								return
							}

							batchSize := int(atomic.LoadInt32(&currentBatch))
							urls := queue.PopBatch(batchSize)

							if len(urls) == 0 {
								time.Sleep(250 * time.Millisecond)
								continue
							}

							for _, url := range urls {
								if myID >= atomic.LoadInt32(&activeWorkers) {
									return
								}

								start := time.Now()
								links, bytes, err := CrawlPage(url)
								duration := time.Since(start)

								telemetry.Record(duration, bytes, err)

								if err == nil && len(links) > 0 {
									queue.Push(links)
								}

								adjust := pid.Compute(int64(duration))
								if adjust < 0 {
									pidThrottle = time.Duration(math.Abs(adjust)/1e6) * time.Millisecond
									if pidThrottle > 2*time.Second {
										pidThrottle = 2 * time.Second
									}
								} else {
									pidThrottle = 0
								}

								baseDelay := time.Duration(atomic.LoadInt64(&currentMinDelay))
								time.Sleep(baseDelay + pidThrottle)
							}
						}
					}(id)
				}
			} else {
				// Scale Down path via safe limit decrements
				if atomic.CompareAndSwapInt32(&activeWorkers, current, current-1) {
					// Surviving workers recognize the new limit and drop out naturally
				}
			}
		}
	}

	adjustWorkers(currentStrategy.MaxWorkers)

	// SYSTEM MONITOR WITH ANNEALED ELITISM PRESERVATION
	go func() {
		recoveryMode := false
		var bestScore float64 = 0.0
		var bestStrategy Strategy = currentStrategy.Clone()

		for {
			time.Sleep(genWindow)

			score := Evaluate(telemetry, genWindow, currentStrategy)
			succ := atomic.LoadUint64(&telemetry.Successes)
			fail := atomic.LoadUint64(&telemetry.Failures)
			bytes := atomic.LoadUint64(&telemetry.BytesRead)

			fmt.Printf("\n[GA GEN EVAL] Fitness: %6.2f | Ingested: %.2f MB | Success: %d | Fail: %d | Workers: %d\n",
				score, float64(bytes)/(1024*1024), succ, fail, atomic.LoadInt32(&activeWorkers))

			total := succ + fail
			var nextStrategy Strategy

			// Elitism selection check
			if score > bestScore && fail == 0 && total > 0 {
				bestScore = score
				bestStrategy = currentStrategy.Clone()
				fmt.Printf("<!> NEW ELITE STRATEGY RECORDED: Score %.2f\n", score)
			}

			if total > 0 && float64(fail)/float64(total) > 0.50 {
				fmt.Println("<!> EMERGENCY: High failure rate detected. Scaling pool down to safety baseline.")
				nextStrategy = currentStrategy.Clone()
				nextStrategy.MaxWorkers = 1
				nextStrategy.BatchSize = 1
				nextStrategy.MinDelay = 2 * time.Second
				recoveryMode = true
			} else if total == 0 {
				fmt.Println("<!> WAITING: Queue starvation or network block. Maintaining baseline probing structure.")
				nextStrategy = currentStrategy.Clone()
				nextStrategy.MaxWorkers = 3
				nextStrategy.BatchSize = 1
			} else if recoveryMode {
				fmt.Println("<!> RECOVERY: System stable. Cautiously probing network by adding a worker.")
				nextStrategy = bestStrategy.Clone() // Pull structural attributes back from elite configuration
				nextStrategy.MaxWorkers = atomic.LoadInt32(&activeWorkers) + 1
				nextStrategy.MinDelay -= 200 * time.Millisecond
				if nextStrategy.MinDelay < 50*time.Millisecond {
					nextStrategy.MinDelay = 50 * time.Millisecond
				}

				if nextStrategy.MaxWorkers >= 4 {
					recoveryMode = false
					fmt.Println("<!> NORMAL OPS: Re-entering normal Darwinian exploration.")
				}
			} else {
				// Exploitation vs Exploration roll
				if rand.Float64() < 0.75 {
					nextStrategy = Mutate(bestStrategy) // Mutate from champion lineage
				} else {
					nextStrategy = bestStrategy.Clone() // Exploit champion exactly to maintain stable baseline
				}
			}

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
