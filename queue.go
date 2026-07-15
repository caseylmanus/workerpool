package main

import (
	"sync"
)

// URLQueue is a thread-safe queue for storing and retrieving URLs to be crawled.
type URLQueue struct {
	mu    sync.Mutex
	links []string
	seen  map[string]bool
}

// NewURLQueue creates a new URLQueue.
func NewURLQueue() *URLQueue {
	return &URLQueue{links: make([]string, 0), seen: make(map[string]bool)}
}

// Push adds new URLs to the queue if they haven't been seen before.
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

// PopBatch retrieves a batch of URLs from the queue.
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

// Len returns the current number of URLs in the queue.
func (q *URLQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.links)
}
