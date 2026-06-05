package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/enums"
	"github.com/warerastats/scraper/internal/gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TradeOffersSnapshot consumes the buy/sell top-orders snapshot for a single
// itemCode. Each order is upserted via UpsertFromAPI; freshly-inserted
// real-ID offers run ReconcileSynthetic so any prior trade-derived
// placeholder for the same (userID, itemCode, side, since) is folded in.
// After processing each side we run MarkCancelledOutsideBand using the seen
// IDs and the worst returned price as the strict band edge.
//
// Every order's userID is enqueued so unknown users get hydrated by the
// users scheduler.
func (in *Ingester) TradeOffersSnapshot(
	ctx context.Context,
	itemCode string,
	limit int,
	buy []gateway.TopOrder,
	sell []gateway.TopOrder,
) {
	in.applyTopOrders(ctx, itemCode, enums.BUY, limit, buy)
	in.applyTopOrders(ctx, itemCode, enums.SELL, limit, sell)
}

func (in *Ingester) applyTopOrders(
	ctx context.Context,
	itemCode string,
	side enums.TradeSide,
	limit int,
	orders []gateway.TopOrder,
) {
	seenIDs := make([]bson.ObjectID, 0, len(orders))
	var worstPrice *float64

	for i := range orders {
		o := orders[i]
		seenIDs = append(seenIDs, o.ID)

		if worstPrice == nil {
			p := o.Price
			worstPrice = &p
		} else {
			switch side {
			case enums.SELL:
				if o.Price > *worstPrice {
					*worstPrice = o.Price
				}
			case enums.BUY:
				if o.Price < *worstPrice {
					*worstPrice = o.Price
				}
			}
		}

		wasInsert, err := in.colls.Trackers.TradeOffer.UpsertFromAPI(
			ctx,
			o.ID,
			o.UserID,
			o.CountryID,
			o.MuID,
			itemCode,
			side,
			o.Quantity,
			o.Price,
			o.OfferAt,
		)
		if err != nil {
			slog.Error("Failed upserting trade offer from API",
				"itemCode", itemCode, "side", side, "offerId", o.ID.Hex(), "error", err)
			continue
		}

		if wasInsert {
			err = in.colls.Trackers.TradeOffer.ReconcileSynthetic(
				ctx, o.ID, o.UserID, itemCode, side, o.OfferAt,
			)
			if err != nil {
				slog.Error("Failed reconciling synthetic trade offer",
					"itemCode", itemCode, "side", side, "offerId", o.ID.Hex(), "error", err)
			}
		}

		in.queue.Enqueue(o.UserID)
	}

	exhaustive := len(orders) < limit
	_, err := in.colls.Trackers.TradeOffer.MarkCancelledOutsideBand(
		ctx, itemCode, side, seenIDs, worstPrice, exhaustive,
	)
	if err != nil {
		slog.Error("Failed marking cancelled trade offers",
			"itemCode", itemCode, "side", side, "error", err)
	}
}

// takerOfferID derives a deterministic ObjectID for the synthetic taker
// offer attached to a trade. It's distinct from tradeID (which is used as
// the synthetic maker offer ID when no maker offer can be matched), but
// reproducible so re-ingesting the same trade is idempotent.
func takerOfferID(tradeID bson.ObjectID) bson.ObjectID {
	var id bson.ObjectID
	copy(id[:], tradeID[:])
	id[len(id)-1] ^= 0x80
	return id
}

// resolveOfferMaker tries to identify which side of a trade was the maker
// by looking up an existing offer keyed on (itemCode, side, since=offerAt,
// userID). It tries seller-as-SELL-maker first, then buyer-as-BUY-maker.
// Returns the matched offer's _id and side, or (zero, zero, false) when
// neither side has a matching offer in the store.
func (in *Ingester) resolveOfferMaker(
	ctx context.Context,
	itemCode string,
	offerAt time.Time,
	sellerID bson.ObjectID,
	buyerID bson.ObjectID,
) (bson.ObjectID, enums.TradeSide, bool) {
	offer, err := in.colls.Trackers.TradeOffer.FindByMatch(ctx, itemCode, enums.SELL, offerAt, sellerID)
	if err == nil {
		return offer.ID, enums.SELL, true
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed looking up seller-side maker offer",
			"itemCode", itemCode, "sellerId", sellerID.Hex(), "error", err)
	}

	offer, err = in.colls.Trackers.TradeOffer.FindByMatch(ctx, itemCode, enums.BUY, offerAt, buyerID)
	if err == nil {
		return offer.ID, enums.BUY, true
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed looking up buyer-side maker offer",
			"itemCode", itemCode, "buyerId", buyerID.Hex(), "error", err)
	}

	return bson.ObjectID{}, "", false
}
