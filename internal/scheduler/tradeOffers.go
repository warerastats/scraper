package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
)

// fungibleItemCodes are the resource item codes whose buy/sell offer books
// we mirror via tradingOrder.getTopOrders.
var fungibleItemCodes = []string{
	"ammo",
	"bread",
	"case1",
	"case2",
	"coca",
	"cocain",
	"concrete",
	"cookedFish",
	"fish",
	"grain",
	"heavyAmmo",
	"iron",
	"lead",
	"lightAmmo",
	"limestone",
	"livestock",
	"oil",
	"petroleum",
	"scraps",
	"steak",
	"steel",
}

// TradeOffers polls the top buy/sell offers for every fungible item code on
// each tick. Calls are issued sequentially with an inter-call sleep of
// `Interval / N` so the burst is spread across the polling window rather
// than fired all at once. The gateway's batcher and rate limiter handle the
// actual upstream pacing.
type TradeOffers struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Limit    int
}

func (s *TradeOffers) Run(ctx context.Context) error {
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

func (s *TradeOffers) tick(ctx context.Context) {
	codes := fungibleItemCodes
	if len(codes) == 0 {
		return
	}

	stagger := s.Interval / time.Duration(len(codes))

	for i, code := range codes {
		select {
		case <-ctx.Done():
			return
		default:
		}

		buy, sell, err := s.Client.GetTopOrders(ctx, code, s.Limit)
		if err != nil {
			slog.Error("Failed getting top orders", "itemCode", code, "error", err)
		} else {
			s.Ingester.TradeOffersSnapshot(ctx, code, s.Limit, buy, sell)
		}

		if stagger > 0 && i < len(codes)-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(stagger):
			}
		}
	}
}
