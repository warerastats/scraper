package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// MuRefresh keeps a steady population of mu-refresh requests in flight on the
// gateway. On every tick it tops the in-flight set up to Target by asking the
// store for fresh candidates that are not already being refreshed. Disbanded
// mus are skipped by the store until they are seen again.
type MuRefresh struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Target   int

	mu       sync.Mutex
	inFlight map[bson.ObjectID]struct{}
}

func (s *MuRefresh) Run(ctx context.Context) error {
	s.inFlight = make(map[bson.ObjectID]struct{}, s.Target)

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		s.tick(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *MuRefresh) tick(ctx context.Context) {
	s.mu.Lock()
	pending := len(s.inFlight)
	need := s.Target - pending
	exclude := make([]bson.ObjectID, 0, pending)
	for id := range s.inFlight {
		exclude = append(exclude, id)
	}
	s.mu.Unlock()

	if need <= 0 {
		return
	}

	ids, err := s.Colls.Trackers.Mu.GetForRefresh(ctx, need, exclude)
	if err != nil {
		slog.Error("Failed getting mus to refresh", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	s.mu.Lock()
	for _, id := range ids {
		s.inFlight[id] = struct{}{}
	}
	s.mu.Unlock()

	for _, id := range ids {
		go s.refreshOne(ctx, id)
	}

	slog.Info("Mu refresh tick: dispatched", "added", len(ids), "inFlight", pending+len(ids), "target", s.Target)
}

func (s *MuRefresh) refreshOne(ctx context.Context, id bson.ObjectID) {
	defer func() {
		s.mu.Lock()
		delete(s.inFlight, id)
		s.mu.Unlock()
	}()

	raw, err := s.Client.GetMuForRefresh(ctx, id)
	if err != nil {
		slog.Error("Failed refreshing mu", "id", id.Hex(), "error", err)
		return
	}
	s.Ingester.Mu(ctx, raw)
}
