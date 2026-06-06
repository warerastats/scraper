package muqueue

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/warerastats/models/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Queue batches mu IDs and, on each tick, asks the database which of them are
// not yet tracked, creating empty tracker documents for those.
type Queue struct {
	ch       chan bson.ObjectID
	colls    *models.Collections
	interval time.Duration
	dropped  atomic.Uint64
}

func New(colls *models.Collections, buffer int, interval time.Duration) *Queue {
	return &Queue{
		ch:       make(chan bson.ObjectID, buffer),
		colls:    colls,
		interval: interval,
	}
}

// Enqueue submits a mu ID to the queue. If the buffer is full the ID is
// dropped rather than blocking the producer; drops are counted and reported in
// aggregate by the flush loop instead of logging one line per dropped ID.
func (q *Queue) Enqueue(id bson.ObjectID) {
	select {
	case q.ch <- id:
	default:
		q.dropped.Add(1)
	}
}

// Run consumes the queue on a ticker until ctx is cancelled.
func (q *Queue) Run(ctx context.Context) error {
	ticker := time.NewTicker(q.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			q.flush(ctx)
		}
	}
}

func (q *Queue) flush(ctx context.Context) {
	if dropped := q.dropped.Swap(0); dropped > 0 {
		slog.Warn("mu-exists queue saturated, dropped ids", "count", dropped)
	}

	seen := make(map[bson.ObjectID]struct{})
	var ids []bson.ObjectID
drain:
	for {
		select {
		case id := <-q.ch:
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		default:
			break drain
		}
	}

	if len(ids) == 0 {
		return
	}

	nonExisting, err := q.colls.Trackers.Mu.Exists(ctx, ids)
	if err != nil {
		slog.Error("Failed checking if mu IDs exist", "error", err)
		return
	}

	for _, id := range nonExisting {
		err = q.colls.Trackers.Mu.CreateEmpty(ctx, id)
		if err != nil {
			slog.Error("Failed creating empty mu tracker", "id", id.Hex(), "error", err)
		}
	}
}
