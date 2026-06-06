package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
)

// Countries runs a periodic job that fetches the full list of countries
// from the gateway and dispatches it to the ingester. The ingest pass
// upserts trackers and writes change events for ruling party and
// specialisation when those fields move.
type Countries struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Offset   time.Duration
}

func (s *Countries) Run(ctx context.Context) error {
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

func (s *Countries) tick(ctx context.Context) {
	raw, err := s.Client.GetCountries(ctx)
	if err != nil {
		slog.Error("Failed getting countries", "error", err)
		return
	}
	s.Ingester.Countries(ctx, raw)
}
