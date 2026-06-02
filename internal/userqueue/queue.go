package userqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Queue batches user IDs and, on each tick, asks the database which of them
// are not yet tracked, creating empty tracker documents for those.
type Queue struct {
	ch       chan bson.ObjectID
	colls    *models.Collections
	interval time.Duration
}

func New(colls *models.Collections, buffer int, interval time.Duration) *Queue {
	return &Queue{
		ch:       make(chan bson.ObjectID, buffer),
		colls:    colls,
		interval: interval,
	}
}

// Enqueue submits a user ID to the queue. If the buffer is full the ID is
// dropped with a warning rather than blocking the producer.
func (q *Queue) Enqueue(id bson.ObjectID) {
	select {
	case q.ch <- id:
	default:
		slog.Warn("user-exists queue full, dropping id", "id", id.Hex())
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
	var ids []bson.ObjectID
drain:
	for {
		select {
		case id := <-q.ch:
			ids = append(ids, id)
		default:
			break drain
		}
	}

	if len(ids) == 0 {
		return
	}

	nonExisting, err := q.colls.Trackers.User.Exists(ctx, ids)
	if err != nil {
		slog.Error("Failed checking if user IDs exist", "error", err)
		return
	}

	for _, id := range nonExisting {
		if err := q.colls.Trackers.User.CreateEmpty(ctx, id); err != nil {
			slog.Error("Failed creating empty user tracker", "id", id.Hex(), "error", err)
		}
	}
}
