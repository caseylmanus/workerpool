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

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
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
	return &URLQueue{links: make([]string, 0), seen: make(map[string]bool)}
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
	Successes       uint64
	Failures        uint64
	BytesRead       uint64
	TotalLat        uint64 // Nanoseconds
	CumulativeBytes uint64 // Tracks total volume safely across threads
}

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
	return kp * float64(setpoint-actual)
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

	mbps := (float64(bytes) / (1024 * 1024)) / window.Seconds()
	avgLatNS := float64(lat / total)

	latFactor := 1.0
	targetNS := float64(s.TargetLatency.Nanoseconds())
	if avgLatNS > targetNS {
		latFactor = math.Exp(-(avgLatNS - targetNS) / targetNS)
	}
	return mbps * latFactor * math.Pow(float64(succ)/float64(total), 2)
}

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

// ============================================================================
// 4. PARSING & NETWORK ENGINE
// ============================================================================

var wikiLinkRegex = regexp.MustCompile(`href="/wiki/([^:#\"'<>]+)"`)

func CrawlPage(url string) ([]string, int, error) {
	client := &http.Client{Timeout: 4 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "GopherConUIBot/1.0 (contact@example.com) Go-LLM-Project")

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
	for _, m := range matches {
		if len(m) > 1 && !strings.Contains(m[1], "Wikipedia") {
			discovered = append(discovered, "https://en.wikipedia.org/wiki/"+m[1])
		}
	}
	return discovered, len(bodyBytes), nil
}

// ============================================================================
// 5. RUNTIME ORCHESTRATION WITH REALTIME VISUAL INTERFACE
// ============================================================================

func main() {
	rand.Seed(time.Now().UnixNano())
	queue := NewURLQueue()
	telemetry := &Telemetry{}
	genWindow := 5 * time.Second

	queue.Push([]string{
		"https://en.wikipedia.org/wiki/Artificial_intelligence",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://en.wikipedia.org/wiki/Control_theory",
	})

	// Background Seeder Thread
	go func() {
		for {
			queue.mu.Lock()
			qLen := len(queue.links)
			queue.mu.Unlock()
			if qLen < 5 {
				queue.Push([]string{fmt.Sprintf("https://en.wikipedia.org/wiki/Special:Random?r=%d", rand.Intn(100000))})
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	currentStrategy := Strategy{TargetLatency: 400 * time.Millisecond, MaxWorkers: 4, BatchSize: 1, MinDelay: 100 * time.Millisecond, Kp: 0.2}
	pid := &AtomicPID{setpoint: int64(currentStrategy.TargetLatency), kp: math.Float64bits(currentStrategy.Kp)}

	var activeWorkers int32 = 0
	var workerIDSequence int32 = 0
	var currentBatch int32 = currentStrategy.BatchSize
	var currentMinDelay int64 = int64(currentStrategy.MinDelay)
	var uiMode string = "INITIALIZING"

	// Worker Pool Orchestration Logic (Deterministic ID Based)
	adjustWorkers := func(target int32) {
		for {
			current := atomic.LoadInt32(&activeWorkers)
			if current == target {
				break
			}
			if current < target {
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
								time.Sleep(time.Duration(atomic.LoadInt64(&currentMinDelay)) + pidThrottle)
							}
						}
					}(id)
				}
			} else {
				if atomic.CompareAndSwapInt32(&activeWorkers, current, current-1) {
				}
			}
		}
	}

	adjustWorkers(currentStrategy.MaxWorkers)

	// GA EVOLUTION THREAD WITH ANNEALED ELITISM
	go func() {
		recoveryMode := false
		var bestScore float64 = 0.0
		var bestStrategy Strategy = currentStrategy.Clone()
		uiMode = "DARWINIAN SEARCH"

		for {
			time.Sleep(genWindow)
			score := Evaluate(telemetry, genWindow, currentStrategy)
			succ := atomic.LoadUint64(&telemetry.Successes)
			fail := atomic.LoadUint64(&telemetry.Failures)
			total := succ + fail

			// Elitism check
			if score > bestScore && fail == 0 && total > 0 {
				bestScore = score
				bestStrategy = currentStrategy.Clone()
			}

			var nextStrategy Strategy
			if total > 0 && float64(fail)/float64(total) > 0.50 {
				uiMode = "PANIC / SCALE BACK"
				nextStrategy = currentStrategy.Clone()
				nextStrategy.MaxWorkers = 1
				nextStrategy.BatchSize = 1
				nextStrategy.MinDelay = 2 * time.Second
				recoveryMode = true
			} else if total == 0 {
				uiMode = "QUEUE STARVATION"
				nextStrategy = currentStrategy.Clone()
				nextStrategy.MaxWorkers = 2
			} else if recoveryMode {
				uiMode = "RECOVERY PROBE"
				nextStrategy = bestStrategy.Clone()
				nextStrategy.MaxWorkers = atomic.LoadInt32(&activeWorkers) + 1
				if nextStrategy.MaxWorkers >= 4 {
					recoveryMode = false
					uiMode = "DARWINIAN SEARCH"
				}
			} else {
				uiMode = "DARWINIAN SEARCH"
				if rand.Float64() < 0.75 {
					nextStrategy = Mutate(bestStrategy)
				} else {
					nextStrategy = bestStrategy.Clone()
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

	// ============================================================================
	// INTERFACE RENDERING DESIGN (Fyne)
	// ============================================================================
	myApp := app.New()
	myWindow := myApp.NewWindow("Darwinian Concurrency Supervisor")

	// DNA Sidebar Labels
	lblWorkers := widget.NewLabel("")
	lblLatency := widget.NewLabel("")
	lblBatch := widget.NewLabel("")
	lblDelay := widget.NewLabel("")
	lblKp := widget.NewLabel("")
	lblMode := widget.NewLabel("")

	dnaContainer := widget.NewCard("Current Strategy DNA", "Hot-swapped constants by GA Layer",
		container.NewVBox(
			container.NewHBox(widget.NewLabel("Active Target Pool:"), lblWorkers),
			container.NewHBox(widget.NewLabel("PID Target Latency:"), lblLatency),
			container.NewHBox(widget.NewLabel("Optimal Batch Size:"), lblBatch),
			container.NewHBox(widget.NewLabel("Floor Backoff Delay:"), lblDelay),
			container.NewHBox(widget.NewLabel("Proportional Kp Gain:"), lblKp),
			container.NewHBox(widget.NewLabel("Supervisor Status: "), lblMode),
		),
	)

	// Live Telemetry Panel Labels
	lblTotalIngested := widget.NewLabel("0.00 MB")
	progressReliability := widget.NewProgressBar()
	lblSuccessCount := widget.NewLabel("0")
	lblFailureCount := widget.NewLabel("0")

	telemetryContainer := widget.NewCard("Live Metrics Plane", "Atomic telemetry polling",
		container.NewVBox(
			container.NewHBox(widget.NewLabel("Total Text Corpus Extracted:"), lblTotalIngested),
			widget.NewLabel("Pool Execution Reliability Score:"),
			progressReliability,
			container.NewHBox(widget.NewLabel("Generation Page Ingestions:"), lblSuccessCount),
			container.NewHBox(widget.NewLabel("Dropped Requests / Limits:  "), lblFailureCount),
		),
	)

	mainGrid := container.NewGridWithColumns(2, dnaContainer, telemetryContainer)
	myWindow.SetContent(container.NewVBox(
		widget.NewLabelWithStyle("DARWINIAN CONCURRENCY ENGINE OBSERVER", fyne.TextAlignCenter, fyne.TextStyle{Bold: true}),
		mainGrid,
	))

	// Async Data Refresh UI Loop
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			fyne.Do(func() {
				// Refresh UI Strategy Variables
				lblWorkers.SetText(fmt.Sprintf("%d Goroutines", atomic.LoadInt32(&activeWorkers)))
				lblLatency.SetText(fmt.Sprintf("%v", time.Duration(atomic.LoadInt64(&pid.setpoint))))
				lblBatch.SetText(fmt.Sprintf("%d URLs", atomic.LoadInt32(&currentBatch)))
				lblDelay.SetText(fmt.Sprintf("%v", time.Duration(atomic.LoadInt64(&currentMinDelay))))
				lblKp.SetText(fmt.Sprintf("%.3f", math.Float64frombits(atomic.LoadUint64(&pid.kp))))
				lblMode.SetText(uiMode)

				// Process Cumulative MB from clean thread-safe bytes tally
				rawBytes := atomic.LoadUint64(&telemetry.CumulativeBytes)
				mb := float64(rawBytes) / (1024 * 1024)
				lblTotalIngested.SetText(fmt.Sprintf("%.2f MB", mb))

				succ := atomic.LoadUint64(&telemetry.Successes)
				fail := atomic.LoadUint64(&telemetry.Failures)
				lblSuccessCount.SetText(fmt.Sprintf("%d", succ))
				lblFailureCount.SetText(fmt.Sprintf("%d", fail))

				total := succ + fail
				if total > 0 {
					progressReliability.SetValue(float64(succ) / float64(total))
				} else {
					progressReliability.SetValue(1.0)
				}
			})
		}
	}()

	myWindow.Resize(fyne.NewSize(700, 320))
	myWindow.ShowAndRun()
}
