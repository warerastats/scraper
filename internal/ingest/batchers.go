package ingest

import (
	"context"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/models/models/stores/transactions"
	"github.com/warerastats/scraper/internal/batch"
)

// Batchers bundles the buffered-flush writers for the high-volume insert
// paths. Transaction batchers are drained synchronously by the transactions
// scheduler before it advances its checkpoint (FlushTransactions); the damage
// batcher is drained at the end of every battle-ranking sweep.
type Batchers struct {
	Damage *batch.Flusher[trackers.Damage]

	Case      *batch.Flusher[transactions.CaseTransaction]
	Craft     *batch.Flusher[transactions.CraftTransaction]
	Dismantle *batch.Flusher[transactions.DismantleTransaction]
	Loot      *batch.Flusher[transactions.LootTransaction]
	Market    *batch.Flusher[transactions.MarketTransaction]
	Trade     *batch.Flusher[transactions.TradeTransaction]
	Wage      *batch.Flusher[transactions.WageTransaction]
}

// NewBatchers wires each flusher to its store's bulk-write method. The bundled
// flushers are drained explicitly (transaction batchers by the transactions
// scheduler, the damage batcher at the end of each battle-ranking sweep), so
// none are registered with Run.
func NewBatchers(colls *models.Collections) *Batchers {
	return &Batchers{
		Damage: batch.New("damages", func(ctx context.Context, ds []trackers.Damage) error {
			return colls.Trackers.Damage.BulkCreate(ctx, ds)
		}),
		Case: batch.New("case_transactions", func(ctx context.Context, txs []transactions.CaseTransaction) error {
			return colls.Transactions.CaseTransaction.BulkCreate(ctx, txs)
		}),
		Craft: batch.New("craft_transactions", func(ctx context.Context, txs []transactions.CraftTransaction) error {
			return colls.Transactions.CraftTransaction.BulkCreate(ctx, txs)
		}),
		Dismantle: batch.New("dismantle_transactions", func(ctx context.Context, txs []transactions.DismantleTransaction) error {
			return colls.Transactions.DismantleTransaction.BulkCreate(ctx, txs)
		}),
		Loot: batch.New("loot_transactions", func(ctx context.Context, txs []transactions.LootTransaction) error {
			return colls.Transactions.LootTransaction.BulkCreate(ctx, txs)
		}),
		Market: batch.New("market_transactions", func(ctx context.Context, txs []transactions.MarketTransaction) error {
			return colls.Transactions.MarketTransaction.BulkCreate(ctx, txs)
		}),
		Trade: batch.New("trade_transactions", func(ctx context.Context, txs []transactions.TradeTransaction) error {
			return colls.Transactions.TradeTransaction.BulkCreate(ctx, txs)
		}),
		Wage: batch.New("wage_transactions", func(ctx context.Context, txs []transactions.WageTransaction) error {
			return colls.Transactions.WageTransaction.BulkCreate(ctx, txs)
		}),
	}
}

// FlushTransactions drains every transaction batcher synchronously and returns
// the first error encountered. The transactions scheduler calls this before
// persisting its LastTransaction high-water mark so a crash can never advance
// the checkpoint past transactions that were never written.
func (b *Batchers) FlushTransactions(ctx context.Context) error {
	flushers := []interface {
		Flush(context.Context) error
	}{
		b.Case, b.Craft, b.Dismantle, b.Loot, b.Market, b.Trade, b.Wage,
	}

	var firstErr error
	for _, f := range flushers {
		err := f.Flush(ctx)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
