package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/enums"
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
		BuyerCountryID  *bson.ObjectID `json:"buyerSellerId,omitempty"`
		ItemCode        string         `json:"itemCode"`
		Money           float64        `json:"money"`
		Quantity        int            `json:"quantity"`
		OfferCreatedAt  time.Time      `json:"offerCreatedAt"`
		CreatedAt       time.Time      `json:"createdAt"`
	}
	if err := json.Unmarshal(t.Raw, &transaction); err != nil {
		slog.Error("Failed unmarshalling trade data", "error", err)
		return
	}

	timeTillSale := transaction.CreatedAt.Sub(transaction.OfferCreatedAt).Milliseconds()

	err := in.colls.Transactions.TradeTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.BuyerID,
		transaction.SellerMuID,
		transaction.BuyerMuID,
		transaction.SellerCountryID,
		transaction.BuyerCountryID,
		nil,
		transaction.ItemCode,
		transaction.Money,
		transaction.Quantity,
		timeTillSale,
	)
	if err != nil {
		slog.Error("Failed creating trade transaction", "error", err)
	}

	in.queue.Enqueue(transaction.BuyerID)
	in.queue.Enqueue(transaction.SellerID)
	in.lastSeen.Mark(transaction.BuyerID)
	in.lastSeen.Mark(transaction.SellerID)
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

	err = in.colls.Transactions.MarketTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.BuyerID,
		transaction.Item.ID,
		transaction.Money,
	)
	if err != nil {
		slog.Error("Failed creating market transaction", "error", err)
	}

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

	err = in.colls.Transactions.WageTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.BuyerID,
		transaction.Money,
		transaction.Quantity,
	)
	if err != nil {
		slog.Error("Failed creating new wage transaction", "error", err)
	}

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

	err = in.colls.Transactions.CaseTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.Item.ID,
		transaction.ItemCode,
	)
	if err != nil {
		slog.Error("Failed creating case transaction", "error", err)
	}

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

	err = in.colls.Transactions.CraftTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.Item.ID,
		transaction.Quantity,
	)
	if err != nil {
		slog.Error("Failed creating craft transaction", "error", err)
	}

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

	err = in.colls.Trackers.Item.SetStatus(ctx, transaction.Item.ID, newEnum)
	if err != nil {
		slog.Error("Failed setting new state of item", "error", err)
		return
	}

	err = in.colls.Transactions.DismantleTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.Item.ID,
		transaction.Quantity,
	)
	if err != nil {
		slog.Error("Failed creating dismantle transaction", "error", err)
	}

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

	err = in.colls.Transactions.LootTransaction.Create(
		ctx,
		transaction.ID,
		transaction.BuyerID,
		transaction.Item.ID,
	)
	if err != nil {
		slog.Error("Failed creating loot transaction", "error", err)
	}

	in.queue.Enqueue(transaction.BuyerID)
	in.lastSeen.Mark(transaction.BuyerID)
}
