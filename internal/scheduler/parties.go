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

// Parties runs a periodic job that finds party tracker entries with no scraped
// data yet, fetches their full document from the gateway, and feeds the result
// to the ingester. Concurrency is bounded by Workers.
type Parties struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Workers  int
}

func (s *Parties) Run(ctx context.Context) error {
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

func (s *Parties) tick(ctx context.Context) {
	emptyIDs, err := s.Colls.Trackers.Party.GetEmpty(ctx)
	if err != nil {
		slog.Error("Failed getting empty parties", "error", err)
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
			raw, err := s.Client.GetParty(gctx, id)
			if err != nil {
				slog.Error("Failed scraping party", "id", id.Hex(), "error", err)
				return nil
			}
			s.Ingester.Party(gctx, raw)
			return nil
		})
	}

	_ = g.Wait()
	slog.Info("Scraped empty parties", "count", len(emptyIDs))
}
