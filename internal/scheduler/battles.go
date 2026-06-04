package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/enums"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"
)

// BattleRanking polls /battleRanking.getRanking for every in-scope battle
// (created at or after ScraperState.TrackBattlesFrom and not yet finalised),
// spreading the calls evenly across SweepPeriod so we don't burst on the
// gateway. On each iteration the oldest-polled battle is selected and both
// sides are fetched in parallel. Battles that drop out of scope are
// finalised via Ingester.BattleFinalize before being removed from the map.
type BattleRanking struct {
	Client      *gateway.Client
	Ingester    *ingest.Ingester
	Colls       *models.Collections
	SweepPeriod time.Duration

	mu           sync.Mutex
	lastPolledAt map[bson.ObjectID]time.Time
}

// minTickDelay is the floor on how fast we can poll, regardless of how many
// battles are in scope. Keeps the scheduler from busy-looping when there's
// only a single battle and SweepPeriod is short.
const minTickDelay = 1 * time.Second

func (s *BattleRanking) Run(ctx context.Context) error {
	s.lastPolledAt = make(map[bson.ObjectID]time.Time)

	for {
		delay := s.tick(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// tick performs a single iteration and returns the wait until the next
// iteration. The returned duration is `SweepPeriod / N` clamped to
// minTickDelay, where N is the number of in-scope battles. When N == 0
// the scheduler idles for SweepPeriod.
func (s *BattleRanking) tick(ctx context.Context) time.Duration {
	state := s.Colls.States.ScraperState.Get(ctx)
	if state == nil {
		slog.Error("BattleRanking: failed reading scraper_state")
		return s.SweepPeriod
	}

	ids, err := s.Colls.Trackers.Battle.GetInScope(ctx, state.TrackBattlesFrom)
	if err != nil {
		slog.Error("BattleRanking: failed loading in-scope battles", "error", err)
		return s.SweepPeriod
	}

	inScope := make(map[bson.ObjectID]struct{}, len(ids))
	for _, id := range ids {
		inScope[id] = struct{}{}
	}

	s.reconcileTransitions(ctx, inScope)

	if len(ids) == 0 {
		return s.SweepPeriod
	}

	target := s.pickOldest(ids)
	prevPollAt := s.previousPollOrSeed(target)

	g, gctx := errgroup.WithContext(ctx)
	for _, side := range []enums.Side{enums.ATTACKER, enums.DEFENDER} {
		side := side
		g.Go(func() error {
			raw, err := s.Client.GetBattleRanking(gctx, target, side)
			if err != nil {
				slog.Error("BattleRanking: failed fetching ranking",
					"battleId", target.Hex(), "side", side, "error", err)
				return nil
			}
			s.Ingester.BattleRanking(gctx, target, side, raw, prevPollAt, s.Client)
			return nil
		})
	}
	_ = g.Wait()

	s.mu.Lock()
	s.lastPolledAt[target] = time.Now().UTC()
	s.mu.Unlock()

	delay := s.SweepPeriod / time.Duration(len(ids))
	if delay < minTickDelay {
		delay = minTickDelay
	}
	return delay
}

// reconcileTransitions detects battles that we previously polled but are no
// longer in scope. They get finalised (winner / endedAt / final totals)
// before being dropped from the map.
func (s *BattleRanking) reconcileTransitions(ctx context.Context, inScope map[bson.ObjectID]struct{}) {
	s.mu.Lock()
	prevTracked := make(map[bson.ObjectID]time.Time, len(s.lastPolledAt))
	for id, t := range s.lastPolledAt {
		prevTracked[id] = t
	}
	s.mu.Unlock()

	for id, prevPollAt := range prevTracked {
		if _, still := inScope[id]; still {
			continue
		}
		// Out of scope. Either we already finalised it (winnerSide set) and
		// just need to drop it, or it ended unilaterally and we need to call
		// Finalize ourselves.
		battle, err := s.Colls.Trackers.Battle.Get(ctx, id)
		if err != nil {
			slog.Error("BattleRanking: failed loading transitioned battle",
				"battleId", id.Hex(), "error", err)
			s.forget(id)
			continue
		}
		if battle.WinnerSide == nil {
			s.Ingester.BattleFinalize(ctx, id, prevPollAt, s.Client)
		}
		s.forget(id)
	}
}

// pickOldest returns the in-scope battle ID whose lastPolledAt is the oldest
// (zero time = highest priority for first-sight battles). If multiple share
// the same age, the order is undefined which is fine — the next tick fixes it.
func (s *BattleRanking) pickOldest(ids []bson.ObjectID) bson.ObjectID {
	s.mu.Lock()
	defer s.mu.Unlock()

	target := ids[0]
	oldest := s.lastPolledAt[target]
	for _, id := range ids[1:] {
		t := s.lastPolledAt[id]
		if t.Before(oldest) {
			oldest = t
			target = id
		}
	}
	return target
}

// previousPollOrSeed returns the timestamp the dismantle-history window
// should anchor on for `id`. For first-sight battles we seed with
// `now - SweepPeriod` so we don't reach back to the dawn of time.
func (s *BattleRanking) previousPollOrSeed(id bson.ObjectID) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.lastPolledAt[id]
	if !ok || t.IsZero() {
		return time.Now().UTC().Add(-s.SweepPeriod)
	}
	return t
}

func (s *BattleRanking) forget(id bson.ObjectID) {
	s.mu.Lock()
	delete(s.lastPolledAt, id)
	s.mu.Unlock()
}
