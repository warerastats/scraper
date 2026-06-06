package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"golang.org/x/sync/errgroup"
)

// BattleReconcile is a safety-net scheduler that finalises battles which the
// region reconciliation path marked inactive (active: false) but were never
// finalised (winnerSide still nil, damages / endedAt unset).
//
// This happens when a battle ends while BattleRanking's in-memory tracking map
// (lastPolledAt) does not contain it — most commonly across a scraper restart,
// since that map is process-local and rebuilt empty on every start. In that
// case reconcileTransitions never calls BattleFinalize, so the tracker is left
// inactive-but-blank. This scheduler periodically sweeps for those battles and
// runs BattleFinalize on each.
type BattleReconcile struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Offset   time.Duration
	Lookback time.Duration
	Workers  int
}

func (s *BattleReconcile) Run(ctx context.Context) error {
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

func (s *BattleReconcile) tick(ctx context.Context) {
	ids, err := s.Colls.Trackers.Battle.GetUnfinalized(ctx)
	if err != nil {
		slog.Error("BattleReconcile: failed loading unfinalized battles", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	// prevPollAt bounds the dismantle-history window for equipment attribution
	// in the final ranking sweep. We have no per-battle poll history here, so
	// anchor on now - Lookback.
	prevPollAt := time.Now().UTC().Add(-s.Lookback)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.Workers)
	for _, id := range ids {
		id := id
		g.Go(func() error {
			s.Ingester.BattleFinalize(gctx, id, prevPollAt, s.Client)
			return nil
		})
	}
	_ = g.Wait()

	slog.Info("BattleReconcile: swept unfinalized battles", "count", len(ids))
}
