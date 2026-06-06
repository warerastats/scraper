package batch

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Flusher buffers items of type T and writes them in batches via the supplied
// flush function. It is safe for concurrent use: Add may be called from many
// goroutines, and Flush / Run serialise their drains under the same mutex.
//
// Delivery semantics are at-most-once per flush attempt: if the flush function
// returns an error the drained batch is dropped (and logged). Callers that need
// at-least-once persistence must gate their progress marker on a successful
// explicit Flush.
type Flusher[T any] struct {
	name  string
	flush func(context.Context, []T) error

	mu      sync.Mutex
	pending []T
}

// New builds a Flusher. name is used only for log context.
func New[T any](name string, flush func(context.Context, []T) error) *Flusher[T] {
	return &Flusher[T]{
		name:  name,
		flush: flush,
	}
}

// Add appends an item to the buffer. It never blocks on the database; the
// accumulated items are written by the next Flush or Run tick.
func (f *Flusher[T]) Add(item T) {
	f.mu.Lock()
	f.pending = append(f.pending, item)
	f.mu.Unlock()
}

// Flush drains the buffer and writes it synchronously, returning the flush
// error (if any) so callers can decide whether to advance a checkpoint. A
// no-op when the buffer is empty.
func (f *Flusher[T]) Flush(ctx context.Context) error {
	f.mu.Lock()
	if len(f.pending) == 0 {
		f.mu.Unlock()
		return nil
	}
	batch := f.pending
	f.pending = nil
	f.mu.Unlock()

	err := f.flush(ctx, batch)
	if err != nil {
		slog.Error("Batch flush failed", "name", f.name, "count", len(batch), "error", err)
	}
	return err
}

// Run flushes the buffer on every interval tick until ctx is cancelled, then
// performs one final drain (on a detached context) so nothing buffered at
// shutdown is lost.
func (f *Flusher[T]) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = f.Flush(drainCtx)
			cancel()
			return ctx.Err()
		case <-ticker.C:
			_ = f.Flush(ctx)
		}
	}
}
