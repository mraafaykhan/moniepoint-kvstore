package server

import (
	"sync"
	"time"
)

// Event represents a real-time cluster event for SSE streaming.
type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
	Time time.Time   `json:"time"`
}

// EventBus manages SSE subscribers and publishes events to all of them.
type EventBus struct {
	mu      sync.RWMutex
	clients map[chan Event]struct{}
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		clients: make(map[chan Event]struct{}),
	}
}

// Subscribe registers a new client and returns a channel to receive events.
func (eb *EventBus) Subscribe() chan Event {
	ch := make(chan Event, 64)
	eb.mu.Lock()
	eb.clients[ch] = struct{}{}
	eb.mu.Unlock()
	return ch
}

// Unsubscribe removes a client and closes its channel.
func (eb *EventBus) Unsubscribe(ch chan Event) {
	eb.mu.Lock()
	delete(eb.clients, ch)
	close(ch)
	eb.mu.Unlock()
}

// Publish sends an event to all subscribers without blocking.
func (eb *EventBus) Publish(evt Event) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	for ch := range eb.clients {
		select {
		case ch <- evt:
		default:
			// slow subscriber, drop event
		}
	}
}
