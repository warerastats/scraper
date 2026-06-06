package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// PartyRefresh keeps parties up to date through two phases on every tick:
//
//   - Ruling: every country's ruling party is guaranteed a refresh once it is
//     older than RulingMaxAge, at an elevated priority. This runs regardless of
//     the disbanded flag.
//   - Filler: the in-flight set is topped up to Target with the oldest
//     non-disbanded parties at filler priority.
//
// A shared in-flight set prevents the two phases (and consecutive ticks) from
// fetching the same party twice concurrently.
type PartyRefresh struct {
	Client       *gateway.Client
	Ingester     *ingest.Ingester
	Colls        *models.Collections
	Interval     time.Duration
	Offset       time.Duration
	Target       int
	RulingMaxAge time.Duration

	mu       sync.Mutex
	inFlight map[bson.ObjectID]struct{}
}

func (s *PartyRefresh) Run(ctx context.Context) error {
	s.inFlight = make(map[bson.ObjectID]struct{}, s.Target)

	if !waitOffset(ctx, s.Offset) {
		return ctx.Err()
	}

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

func (s *PartyRefresh) tick(ctx context.Context) {
	s.rulingPhase(ctx)
	s.fillerPhase(ctx)
}

// rulingPhase guarantees every country's ruling party is re-fetched once it is
// older than RulingMaxAge.
func (s *PartyRefresh) rulingPhase(ctx context.Context) {
	rulingIDs, err := s.Colls.Trackers.Country.DistinctRulingPartyIDs(ctx)
	if err != nil {
		slog.Error("Failed getting ruling party IDs", "error", err)
		return
	}
	if len(rulingIDs) == 0 {
		return
	}

	stale, err := s.Colls.Trackers.Party.GetStaleAmong(ctx, rulingIDs, s.RulingMaxAge)
	if err != nil {
		slog.Error("Failed getting stale ruling parties", "error", err)
		return
	}

	dispatched := s.claim(stale)
	for _, id := range dispatched {
		go s.fetchOne(ctx, id, true)
	}
	if len(dispatched) > 0 {
		slog.Info("Party ruling refresh: dispatched", "added", len(dispatched))
	}
}

// fillerPhase tops the in-flight set up to Target with the oldest
// non-disbanded parties.
func (s *PartyRefresh) fillerPhase(ctx context.Context) {
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

	ids, err := s.Colls.Trackers.Party.GetForRefresh(ctx, need, exclude)
	if err != nil {
		slog.Error("Failed getting parties to refresh", "error", err)
		return
	}

	dispatched := s.claim(ids)
	for _, id := range dispatched {
		go s.fetchOne(ctx, id, false)
	}
	if len(dispatched) > 0 {
		slog.Info("Party refresh tick: dispatched", "added", len(dispatched), "target", s.Target)
	}
}

// claim adds the given ids to the in-flight set, returning only those that were
// not already in flight.
func (s *PartyRefresh) claim(ids []bson.ObjectID) []bson.ObjectID {
	if len(ids) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	claimed := make([]bson.ObjectID, 0, len(ids))
	for _, id := range ids {
		if _, ok := s.inFlight[id]; ok {
			continue
		}
		s.inFlight[id] = struct{}{}
		claimed = append(claimed, id)
	}
	return claimed
}

func (s *PartyRefresh) fetchOne(ctx context.Context, id bson.ObjectID, ruling bool) {
	defer func() {
		s.mu.Lock()
		delete(s.inFlight, id)
		s.mu.Unlock()
	}()

	var (
		raw json.RawMessage
		err error
	)
	if ruling {
		raw, err = s.Client.GetPartyForRuling(ctx, id)
	} else {
		raw, err = s.Client.GetPartyForRefresh(ctx, id)
	}
	if err != nil {
		slog.Error("Failed refreshing party", "id", id.Hex(), "ruling", ruling, "error", err)
		return
	}
	if len(raw) == 0 {
		err = s.Colls.Trackers.Party.MarkDisbanded(ctx, id)
		if err != nil {
			slog.Error("Failed marking vanished party disbanded", "id", id.Hex(), "error", err)
		}
		return
	}
	s.Ingester.Party(ctx, raw)
}
