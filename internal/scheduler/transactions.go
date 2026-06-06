package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
)

// Transactions runs a periodic job that pulls new transactions from the
// gateway and dispatches them to the ingester via a bounded worker pool.
type Transactions struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Offset   time.Duration
	Workers  int
}

func (s *Transactions) Run(ctx context.Context) error {
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

func (s *Transactions) tick(ctx context.Context) {
	state := s.Colls.States.ScraperState.Get(ctx)
	slog.Info("Running transaction scraper", "from", state.LastTransaction)

	transactions, err := s.Client.GetTransactions(ctx, state.LastTransaction)
	if err != nil {
		slog.Error("Failed getting transactions", "error", err)
		return
	}

	lastCreated := state.LastTransaction
	sem := make(chan struct{}, s.Workers)
	var wg sync.WaitGroup

	for _, item := range transactions {
		if item.CreatedAt.After(lastCreated) {
			lastCreated = item.CreatedAt
		}

		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(it gateway.Transaction) {
			defer wg.Done()
			defer func() { <-sem }()
			s.Ingester.Transaction(ctx, it)
		}(item)
	}

	wg.Wait()

	// Persist every buffered transaction from this tick before advancing the
	// checkpoint. If the flush fails we leave LastTransaction where it is so
	// the next tick re-fetches and re-ingests (idempotent) rather than skipping
	// transactions that were never written.
	err = s.Ingester.FlushTransactions(ctx)
	if err != nil {
		slog.Error("Failed flushing transactions; not advancing checkpoint", "error", err)
		return
	}

	state.SetLastTransaction(ctx, lastCreated)
	slog.Info("Gotten new transactions", "count", len(transactions))
}
