package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"
)

// Companies runs a periodic workforce refresh. On each tick it inspects the
// wage_transactions written since the last successful sweep, derives the set
// of companies that paid wages from the workers' user records, then refreshes
// each company and its worker roster via the gateway. The window is clamped
// to BackfillMax to bound recovery work after downtime.
type Companies struct {
	Client      *gateway.Client
	Ingester    *ingest.Ingester
	Colls       *models.Collections
	Interval    time.Duration
	Offset      time.Duration
	BackfillMax time.Duration
	Workers     int
}

func (s *Companies) Run(ctx context.Context) error {
	// Align the first tick to the next wall-clock boundary so subsequent
	// ticks fire at :00, :10, :20, ... (assuming a 10-minute Interval). The
	// Offset shifts the firing time off the boundary so the request doesn't
	// land on top of the other schedulers, while the window stays aligned.
	next := time.Now().UTC().Truncate(s.Interval).Add(s.Interval).Add(s.Offset)
	slog.Info("Companies scheduler aligning to wall clock", "firstTick", next)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Until(next)):
	}

	s.tick(ctx)

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Companies) tick(ctx context.Context) {
	state := s.Colls.States.ScraperState.Get(ctx)
	if state == nil {
		return
	}

	// Snap the upper bound to the most recent boundary so the window is
	// deterministic even if the ticker fires a few ms late.
	now := time.Now().UTC().Truncate(s.Interval)
	since := state.TrackEmploymentFrom
	if since.IsZero() || now.Sub(since) > s.BackfillMax {
		since = now.Add(-s.BackfillMax)
	}

	slog.Info("Running companies scraper", "from", since, "to", now)

	userIDs, err := s.Colls.Transactions.WageTransaction.DistinctEmployees(ctx, since, now)
	if err != nil {
		slog.Error("Failed getting distinct wage employees", "error", err)
		return
	}
	if len(userIDs) == 0 {
		state.SetTrackEmploymentFrom(ctx, now)
		return
	}

	companyIDs, err := s.Colls.Trackers.User.DistinctCompanyIDs(ctx, userIDs)
	if err != nil {
		slog.Error("Failed getting distinct company IDs", "error", err)
		return
	}
	if len(companyIDs) == 0 {
		state.SetTrackEmploymentFrom(ctx, now)
		return
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.Workers)

	for _, id := range companyIDs {
		id := id
		g.Go(func() error {
			s.processCompany(gctx, id)
			return nil
		})
	}

	_ = g.Wait()

	state.SetTrackEmploymentFrom(ctx, now)
	slog.Info("Refreshed companies",
		"employees", len(userIDs),
		"companies", len(companyIDs),
	)
}

func (s *Companies) processCompany(ctx context.Context, companyID bson.ObjectID) {
	prevEmployees, err := s.Colls.Trackers.Employee.GetByCompany(ctx, companyID)
	if err != nil {
		slog.Error("Failed loading prior employees", "companyId", companyID.Hex(), "error", err)
	}

	var companyRaw, workersRaw []byte
	cg, cgctx := errgroup.WithContext(ctx)
	cg.Go(func() error {
		raw, err := s.Client.GetCompany(cgctx, companyID)
		if err != nil {
			return err
		}
		companyRaw = raw
		return nil
	})
	cg.Go(func() error {
		raw, err := s.Client.GetWorkers(cgctx, companyID)
		if err != nil {
			return err
		}
		workersRaw = raw
		return nil
	})

	err = cg.Wait()
	if err != nil {
		slog.Error("Failed fetching company data", "companyId", companyID.Hex(), "error", err)
		return
	}

	s.Ingester.Company(ctx, companyRaw)
	currentUserIDs := s.Ingester.Workers(ctx, workersRaw, companyID)

	current := make(map[bson.ObjectID]struct{}, len(currentUserIDs))
	for _, id := range currentUserIDs {
		current[id] = struct{}{}
	}
	for _, prev := range prevEmployees {
		if _, ok := current[prev.UserID]; ok {
			continue
		}
		s.Ingester.MarkWorkerLeft(ctx, prev)
	}
}
