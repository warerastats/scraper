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

// Users runs a periodic job that finds tracker entries with no scraped data
// yet, fetches their full profile from the gateway, and feeds the result to
// the ingester. Concurrency is bounded by Workers.
type Users struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Workers  int
}

func (s *Users) Run(ctx context.Context) error {
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

func (s *Users) tick(ctx context.Context) {
	emptyIDs, err := s.Colls.Trackers.User.GetEmpty(ctx)
	if err != nil {
		slog.Error("Failed getting empty users", "error", err)
		return
	}
	if len(emptyIDs) == 0 {
		return
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.Workers)

	for _, id := range emptyIDs {
		id := id
		g.Go(func() error {
			raw, err := s.Client.GetUser(gctx, id)
			if err != nil {
				slog.Error("Failed scraping user", "id", id.Hex(), "error", err)
				return nil
			}
			s.Ingester.User(gctx, raw)
			return nil
		})
	}

	_ = g.Wait()
	slog.Info("Scraped empty users", "count", len(emptyIDs))
}
