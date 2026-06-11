package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/enums"
	"github.com/warerastats/models/models/stores/transactions"
	"github.com/warerastats/scraper/internal/gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Transaction dispatches a transaction payload to the right handler based on
// its type. Unknown types are logged and discarded.
func (in *Ingester) Transaction(ctx context.Context, t gateway.Transaction) {
	switch t.TransactionType {
	case "trading":
		in.trading(ctx, t)
	case "itemMarket":
		in.itemMarket(ctx, t)
	case "wage":
		in.wage(ctx, t)
	case "openCase":
		in.openCase(ctx, t)
	case "craftItem":
		in.craftItem(ctx, t)
	case "dismantleItem":
		in.dismantleItem(ctx, t)
	case "battleLoot":
		in.battleLoot(ctx, t)
	default:
		slog.Warn("Unknown transaction type", "type", t.TransactionType)
	}
}

// ensureItem upserts an item tracker. Item.Create is itself an upsert, so
// any pre-existing placeholder (created by user-equipment ingest) is fully
// populated on the first transaction that surfaces the item.
func (in *Ingester) ensureItem(ctx context.Context, item Item, owner bson.ObjectID) error {
	return in.colls.Trackers.Item.Create(ctx, item.ID, item.Code, item.Skills, owner)
}

func (in *Ingester) trading(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID              bson.ObjectID  `json:"_id"`
		SellerID        bson.ObjectID  `json:"sellerId"`
		BuyerID         bson.ObjectID  `json:"buyerId"`
		SellerMuID      *bson.ObjectID `json:"sellerMuId,omitempty"`
		BuyerMuID       *bson.ObjectID `json:"buyerMuId,omitempty"`
		SellerCountryID *bson.ObjectID `json:"sellerCountryId,omitempty"`
		BuyerCountryID  *bson.ObjectID `json:"buyerCountryId,omitempty"`
		ItemCode        string         `json:"itemCode"`
		Money           float64        `json:"money"`
		Quantity        int            `json:"quantity"`
		OfferCreatedAt  time.Time      `json:"offerCreatedAt"`
		CreatedAt       time.Time      `json:"createdAt"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling trade data", "error", err)
		return
	}

	timeTillSale := transaction.CreatedAt.Sub(transaction.OfferCreatedAt).Milliseconds()

	in.batchers.Trade.Add(transactions.TradeTransaction{
		ID:              transaction.ID,
		SellerID:        transaction.SellerID,
		BuyerID:         transaction.BuyerID,
		SellerMuID:      transaction.SellerMuID,
		BuyerMuID:       transaction.BuyerMuID,
		SellerCountryID: transaction.SellerCountryID,
		BuyerCountryID:  transaction.BuyerCountryID,
		ItemOfferID:     nil,
		ItemCode:        transaction.ItemCode,
		Money:           transaction.Money,
		Quantity:        transaction.Quantity,
		TimeTillSale:    timeTillSale,
	})

	in.applyTradeToOffers(ctx, transaction.ID,
		transaction.ItemCode, transaction.Quantity, transaction.Money,
		transaction.OfferCreatedAt, transaction.CreatedAt,
		transaction.SellerID, transaction.BuyerID,
		transaction.SellerCountryID, transaction.BuyerCountryID,
		transaction.SellerMuID, transaction.BuyerMuID,
	)

	in.queue.Enqueue(transaction.BuyerID)
	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.BuyerID)
	in.lastSeen.Mark(transaction.SellerID)
}

// applyTradeToOffers updates the maker's offer (real if matched, synthetic
// keyed on tradeID otherwise) and always synthesises a fully-filled taker
// offer keyed on takerOfferID(tradeID). Maker price is the trade's
// per-unit price (money/quantity); the taker side's "offer" is recorded
// at the same per-unit price for analytical symmetry.
func (in *Ingester) applyTradeToOffers(
	ctx context.Context,
	tradeID bson.ObjectID,
	itemCode string,
	quantity int,
	money float64,
	offerCreatedAt time.Time,
	createdAt time.Time,
	sellerID bson.ObjectID,
	buyerID bson.ObjectID,
	sellerCountryID *bson.ObjectID,
	buyerCountryID *bson.ObjectID,
	sellerMuID *bson.ObjectID,
	buyerMuID *bson.ObjectID,
) {
	if quantity <= 0 {
		return
	}
	unitPrice := money / float64(quantity)

	makerID, makerSide, matched := in.resolveOfferMaker(
		ctx, itemCode, offerCreatedAt, sellerID, buyerID,
	)

	// Maker side info: when matched we know which side; otherwise default to
	// seller-as-SELL-maker per the workspace decision.
	if !matched {
		makerSide = enums.SELL
	}

	var (
		makerUserID    bson.ObjectID
		makerCountryID *bson.ObjectID
		makerMuID      *bson.ObjectID
		takerUserID    bson.ObjectID
		takerCountryID *bson.ObjectID
		takerMuID      *bson.ObjectID
		takerSide      enums.TradeSide
	)
	switch makerSide {
	case enums.SELL:
		makerUserID, makerCountryID, makerMuID = sellerID, sellerCountryID, sellerMuID
		takerUserID, takerCountryID, takerMuID = buyerID, buyerCountryID, buyerMuID
		takerSide = enums.BUY
	case enums.BUY:
		makerUserID, makerCountryID, makerMuID = buyerID, buyerCountryID, buyerMuID
		takerUserID, takerCountryID, takerMuID = sellerID, sellerCountryID, sellerMuID
		takerSide = enums.SELL
	}

	// Maker: increment fulfillment on a known offer, or synthesise one
	// keyed on tradeID.
	if matched {
		err := in.colls.Trackers.TradeOffer.RecordFill(ctx, makerID, quantity)
		if err != nil {
			slog.Error("Failed recording fill on maker offer",
				"offerId", makerID.Hex(), "tradeId", tradeID.Hex(), "error", err)
		}
	} else {
		err := in.colls.Trackers.TradeOffer.CreateSynthetic(
			ctx, tradeID, makerUserID, makerCountryID, makerMuID,
			itemCode, makerSide, unitPrice, offerCreatedAt, quantity,
		)
		if err != nil {
			slog.Error("Failed creating synthetic maker offer",
				"tradeId", tradeID.Hex(), "error", err)
		}
	}

	// Taker: always synthesise an instant offer at createdAt.
	err := in.colls.Trackers.TradeOffer.CreateSynthetic(
		ctx, takerOfferID(tradeID), takerUserID, takerCountryID, takerMuID,
		itemCode, takerSide, unitPrice, createdAt, quantity,
	)
	if err != nil {
		slog.Error("Failed creating synthetic taker offer",
			"tradeId", tradeID.Hex(), "error", err)
	}
}

func (in *Ingester) itemMarket(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		BuyerID  bson.ObjectID `json:"buyerId"`
		Item     Item          `json:"item"`
		Money    float64       `json:"money"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling item market data", "error", err)
		return
	}

	err = in.ensureItem(ctx, transaction.Item, transaction.BuyerID)
	if err != nil {
		slog.Error("Failed ensuring item", "object id", transaction.Item.ID, "error", err)
		return
	}

	in.batchers.Market.Add(transactions.MarketTransaction{
		ID:       transaction.ID,
		SellerID: transaction.SellerID,
		BuyerID:  transaction.BuyerID,
		ItemID:   transaction.Item.ID,
		ItemCode: transaction.Item.Code,
		Money:    transaction.Money,
	})

	in.queue.Enqueue(transaction.BuyerID)
	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.BuyerID)
	in.lastSeen.Mark(transaction.SellerID)
}

func (in *Ingester) wage(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		BuyerID  bson.ObjectID `json:"buyerId"`
		Quantity int           `json:"quantity"`
		Money    float64       `json:"money"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling wage data", "error", err)
		return
	}

	in.batchers.Wage.Add(transactions.WageTransaction{
		ID:               transaction.ID,
		EmployeeID:       transaction.SellerID,
		EmployerID:       transaction.BuyerID,
		Money:            transaction.Money,
		ProductionPoints: transaction.Quantity,
	})

	in.queue.Enqueue(transaction.BuyerID)
	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.BuyerID)
	in.lastSeen.Mark(transaction.SellerID)
}

func (in *Ingester) openCase(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		Item     Item          `json:"item"`
		ItemCode string        `json:"itemCode"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling open case data", "error", err)
		return
	}

	err = in.ensureItem(ctx, transaction.Item, transaction.SellerID)
	if err != nil {
		slog.Error("Failed ensuring item", "object id", transaction.Item.ID, "error", err)
		return
	}

	in.batchers.Case.Add(transactions.CaseTransaction{
		ID:     transaction.ID,
		UserID: transaction.SellerID,
		ItemID: transaction.Item.ID,
		Case:   transaction.ItemCode,
	})

	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.SellerID)
}

func (in *Ingester) craftItem(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		Item     Item          `json:"item"`
		Quantity int           `json:"quantity"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling craft item data", "error", err)
		return
	}

	err = in.ensureItem(ctx, transaction.Item, transaction.SellerID)
	if err != nil {
		slog.Error("Failed ensuring item", "object id", transaction.Item.ID, "error", err)
		return
	}

	in.batchers.Craft.Add(transactions.CraftTransaction{
		ID:         transaction.ID,
		UserID:     transaction.SellerID,
		ItemID:     transaction.Item.ID,
		ScrapsCost: transaction.Quantity,
	})

	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.SellerID)
}

func (in *Ingester) dismantleItem(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		Item     Item          `json:"item"`
		Quantity int           `json:"quantity"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling dismantle item data", "error", err)
		return
	}

	err = in.ensureItem(ctx, transaction.Item, transaction.SellerID)
	if err != nil {
		slog.Error("Failed ensuring item", "object id", transaction.Item.ID, "error", err)
		return
	}

	newEnum := enums.DISMANTLED
	if transaction.Item.State == 0 {
		newEnum = enums.BROKEN
	}

	err = in.colls.Trackers.Item.SetStatusAndState(ctx, transaction.Item.ID, newEnum, transaction.Item.State)
	if err != nil {
		slog.Error("Failed setting new state of item", "error", err)
		return
	}

	in.batchers.Dismantle.Add(transactions.DismantleTransaction{
		ID:             transaction.ID,
		UserID:         transaction.SellerID,
		ItemID:         transaction.Item.ID,
		ScrapsReceived: transaction.Quantity,
	})

	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.SellerID)
}

func (in *Ingester) battleLoot(ctx context.Context, t gateway.Transaction) {
	var transaction struct {
		ID      bson.ObjectID `json:"_id"`
		BuyerID bson.ObjectID `json:"buyerId"`
		Item    Item          `json:"item"`
	}

	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling battle loot data", "error", err)
		return
	}

	err = in.ensureItem(ctx, transaction.Item, transaction.BuyerID)
	if err != nil {
		slog.Error("Failed ensuring item", "object id", transaction.Item.ID, "error", err)
		return
	}

	in.batchers.Loot.Add(transactions.LootTransaction{
		ID:     transaction.ID,
		UserID: transaction.BuyerID,
		ItemID: transaction.Item.ID,
	})

	in.queue.Enqueue(transaction.BuyerID)
	in.lastSeen.Mark(transaction.BuyerID)
}
