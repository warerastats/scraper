package lastseen

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// MuFlusher batches mu IDs that have been "seen" by the ingestion pipeline
// (via an active member) and, on each tick, bulk-updates their lastSeen field
// in one DB write. A mu whose lastSeen overtakes disbandedAt is thereby revived
// for refresh.
type MuFlusher struct {
	colls    *models.Collections
	interval time.Duration

	mu  sync.Mutex
	ids map[bson.ObjectID]struct{}
}

func NewMuFlusher(colls *models.Collections, interval time.Duration) *MuFlusher {
	return &MuFlusher{
		colls:    colls,
		interval: interval,
		ids:      make(map[bson.ObjectID]struct{}),
	}
}

// Mark records that a mu ID has been seen. Safe to call from multiple
// goroutines. Repeated calls within a tick collapse to a single DB update.
func (f *MuFlusher) Mark(id bson.ObjectID) {
	f.mu.Lock()
	f.ids[id] = struct{}{}
	f.mu.Unlock()
}

// Run flushes the pending set on every tick until ctx is cancelled.
func (f *MuFlusher) Run(ctx context.Context) error {
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

func (f *MuFlusher) flush(ctx context.Context) {
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

	err := f.colls.Trackers.Mu.MarkLastSeen(ctx, ids)
	if err != nil {
		slog.Error("Failed bulk updating mu lastSeen", "count", len(ids), "error", err)
	}
}
