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

// Mus runs a periodic job that finds mu tracker entries with no scraped data
// yet, fetches their full document from the gateway, and feeds the result to
// the ingester. Concurrency is bounded by Workers.
type Mus struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Workers  int
}

func (s *Mus) Run(ctx context.Context) error {
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

func (s *Mus) tick(ctx context.Context) {
	emptyIDs, err := s.Colls.Trackers.Mu.GetEmpty(ctx)
	if err != nil {
		slog.Error("Failed getting empty mus", "error", err)
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
			raw, err := s.Client.GetMu(gctx, id)
			if err != nil {
				slog.Error("Failed scraping mu", "id", id.Hex(), "error", err)
				return nil
			}
			s.Ingester.Mu(gctx, raw)
			return nil
		})
	}

	_ = g.Wait()
	slog.Info("Scraped empty mus", "count", len(emptyIDs))
}
