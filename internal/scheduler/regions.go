package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
)

// Regions runs a periodic job that fetches the full region object from the
// gateway and dispatches it to the ingester. The ingest pass also handles
// active-battle tracking and end-of-tick deactivation reconciliation.
type Regions struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
}

func (s *Regions) Run(ctx context.Context) error {
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

func (s *Regions) tick(ctx context.Context) {
	raw, err := s.Client.GetRegions(ctx)
	if err != nil {
		slog.Error("Failed getting regions", "error", err)
		return
	}
	s.Ingester.Regions(ctx, raw)
}
