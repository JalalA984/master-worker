// Package events provides a pub/sub event bus for real-time WebSocket broadcast.
//
// The event bus decouples event producers (task enqueue, completion, worker
// connect/disconnect) from consumers (WebSocket clients, dashboard).
//
// This is the Observer pattern (GoF) / Publish-Subscribe messaging pattern:
//   - Producers call Publish() without knowing who's listening
//   - Consumers call Subscribe() to receive events on a channel
//   - When a consumer disconnects, Unsubscribe() cleans up
//
// Real-world systems using pub/sub:
//   - Redis Pub/Sub: used in our google-docs-sim project
//   - Kubernetes: watch API is pub/sub over HTTP streaming
//   - Apache Kafka: distributed pub/sub with persistence
package events

import (
	"encoding/json"
	"sync"
	"time"
)

// EventType categorizes events for filtering.
type EventType string

const (
	EventTaskQueued     EventType = "task_queued"
	EventTaskAssigned   EventType = "task_assigned"
	EventTaskCompleted  EventType = "task_completed"
	EventTaskFailed     EventType = "task_failed"
	EventTaskRetrying   EventType = "task_retrying"
	EventTaskDead       EventType = "task_dead"
	EventWorkerConnected    EventType = "worker_connected"
	EventWorkerDisconnected EventType = "worker_disconnected"
	EventWorkerSuspect      EventType = "worker_suspect"
	EventWorkerDead         EventType = "worker_dead"

	// Chaos engineering events.
	EventChaosWorkerKilled EventType = "chaos_worker_killed"
	EventChaosCBTripped    EventType = "chaos_cb_tripped"
	EventChaosCBReset      EventType = "chaos_cb_reset"
	EventBatchSubmitted    EventType = "batch_submitted"
)

// Event represents a system event broadcast to all subscribers.
type Event struct {
	Type      EventType   `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// JSON returns the JSON-encoded event.
func (e Event) JSON() []byte {
	data, _ := json.Marshal(e)
	return data
}

// Bus manages event subscribers and broadcasting.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
}

// NewBus creates a new event bus.
func NewBus() *Bus {
	return &Bus{
		subscribers: make(map[chan Event]struct{}),
	}
}

// Subscribe creates a new subscription channel.
// The caller must call Unsubscribe when done.
func (b *Bus) Subscribe() chan Event {
	// Buffer 512: a 100-task batch generates ~300 events (queued, assigned,
	// completed); 64 was too small and caused silent drops under load.
	ch := make(chan Event, 512)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes a subscription and closes the channel.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	delete(b.subscribers, ch)
	b.mu.Unlock()
	close(ch)
}

// Publish sends an event to all subscribers (non-blocking).
// Slow consumers will miss events rather than blocking producers.
func (b *Bus) Publish(event Event) {
	event.Timestamp = time.Now()
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.subscribers {
		select {
		case ch <- event:
		default:
			// Drop event for slow consumer rather than blocking.
		}
	}
}

// SubscriberCount returns the number of active subscribers.
func (b *Bus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}
