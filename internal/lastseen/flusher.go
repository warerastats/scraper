package lastseen

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Flusher batches user IDs that have been "seen" by the ingestion pipeline
// and, on each tick, bulk-updates their lastSeen field in one DB write. The
// dedupe set bounds the working set per tick, so there's no need for a
// drop-oldest channel like the userqueue.
type Flusher struct {
	colls    *models.Collections
	interval time.Duration

	mu  sync.Mutex
	ids map[bson.ObjectID]struct{}
}

func New(colls *models.Collections, interval time.Duration) *Flusher {
	return &Flusher{
		colls:    colls,
		interval: interval,
		ids:      make(map[bson.ObjectID]struct{}),
	}
}

// Mark records that a user ID has been seen. Safe to call from multiple
// goroutines. Repeated calls within a tick collapse to a single DB update.
func (f *Flusher) Mark(id bson.ObjectID) {
	f.mu.Lock()
	f.ids[id] = struct{}{}
	f.mu.Unlock()
}

// Run flushes the pending set on every tick until ctx is cancelled.
func (f *Flusher) Run(ctx context.Context) error {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			f.flush(ctx)
		}
	}
}

func (f *Flusher) flush(ctx context.Context) {
	f.mu.Lock()
	if len(f.ids) == 0 {
		f.mu.Unlock()
		return
	}
	pending := f.ids
	f.ids = make(map[bson.ObjectID]struct{})
	f.mu.Unlock()

	ids := make([]bson.ObjectID, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}

	if err := f.colls.Trackers.User.MarkLastSeen(ctx, ids); err != nil {
		slog.Error("Failed bulk updating lastSeen", "count", len(ids), "error", err)
	}
}
