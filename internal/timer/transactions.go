package timer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/enums"
	"github.com/warerastats/scraper/internal/scraper"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Item struct {
	ID     bson.ObjectID      `json:"_id"`
	Code   string             `json:"code"`
	Skills map[string]float64 `json:"skills"`
	State  int                `json:"state"`
}

func Transactions(ctx context.Context, colls *models.Collections) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		GetTransactions(ctx, colls)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func GetTransactions(ctx context.Context, colls *models.Collections) {
	state := colls.States.ScraperState.Get(ctx)

	transactions, err := scraper.GetTransactions(state.LastTransaction)
	if err != nil {
		slog.Error("Failed getting transactions", "error", err)
		return
	}

	var latest time.Time
	for _, item := range transactions {
		if item.CreatedAt.After(state.LastTransaction) {
			state.LastTransaction = item.CreatedAt
		}

		switch item.TransactionType {
		case "trading":
			go handleTrading(ctx, colls, item)
		case "itemMarket":
			go handleItemMarket(ctx, colls, item)
		case "wage":
			go handleWage(ctx, colls, item)
		case "openCase":
			go handleOpenCase(ctx, colls, item)
		case "craftItem":
			go handleCraftItem(ctx, colls, item)
		case "dismantleItem":
			go handleDismantleItem(ctx, colls, item)
		case "battleLoot":
			go handleBattleLoot(ctx, colls, item)
		default:
			slog.Warn("Unknown transaction type", "type", item.TransactionType)
		}

		if item.CreatedAt.After(latest) {
			latest = item.CreatedAt
		}
	}

	state.Set(ctx)
	slog.Info("Gotten new transactions", "count", len(transactions))
}

func handleTrading(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
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
	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling trade data", "error", err)
		return
	}

	timeTillSale := transaction.CreatedAt.Sub(transaction.OfferCreatedAt).Milliseconds()

	err = colls.Transactions.TradeTransaction.Create(
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
		int(transaction.Quantity),
		timeTillSale,
	)
	if err != nil {
		slog.Error("Failed creating trade transaction", "error", err)
	}
}

func handleItemMarket(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
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

	exists, err := colls.Trackers.Item.Exists(ctx, transaction.Item.ID)
	if err != nil {
		slog.Error("Failed checking if item exists", "object id", transaction.Item.ID, "error", err)
		return
	}

	if !exists {
		err = colls.Trackers.Item.Create(
			ctx,
			transaction.Item.ID,
			transaction.Item.Code,
			transaction.Item.Skills,
			transaction.BuyerID,
		)
		if err != nil {
			slog.Error("Failed creating new item!", "object id", transaction.Item.ID, "error", err)
			return
		}
	} else {
		err = colls.Trackers.Item.SetOwnerUserID(ctx, transaction.Item.ID, transaction.BuyerID)
		if err != nil {
			slog.Error("Failed updating item owner ID", "object id", transaction.Item.ID, "new owner", transaction.BuyerID, "error", err)
			return
		}
	}

	colls.Transactions.MarketTransaction.Create(
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
}

func handleWage(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
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

	err = colls.Transactions.WageTransaction.Create(
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
}

func handleOpenCase(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		Item     Item          `json:"item"`
		ItemCode string        `json:"itemCode"`
	}
	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling wage data", "error", err)
		return
	}

	exists, err := colls.Trackers.Item.Exists(ctx, transaction.Item.ID)
	if err != nil {
		slog.Error("Failed checking if item exists", "object id", transaction.Item.ID, "error", err)
		return
	}

	if !exists {
		err = colls.Trackers.Item.Create(
			ctx,
			transaction.Item.ID,
			transaction.Item.Code,
			transaction.Item.Skills,
			transaction.SellerID,
		)
		if err != nil {
			slog.Error("Failed creating new item!", "object id", transaction.Item.ID, "error", err)
			return
		}
	}

	err = colls.Transactions.CaseTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.Item.ID,
		transaction.ItemCode,
	)
	if err != nil {
		slog.Error("Failed creating case transaction", "error", err)
	}
}

func handleCraftItem(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		Item     Item          `json:"item"`
		Quantity int           `json:"quantity"`
	}
	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling wage data", "error", err)
		return
	}

	exists, err := colls.Trackers.Item.Exists(ctx, transaction.Item.ID)
	if err != nil {
		slog.Error("Failed checking if item exists", "object id", transaction.Item.ID, "error", err)
		return
	}

	if !exists {
		err = colls.Trackers.Item.Create(
			ctx,
			transaction.Item.ID,
			transaction.Item.Code,
			transaction.Item.Skills,
			transaction.SellerID,
		)
		if err != nil {
			slog.Error("Failed creating new item!", "object id", transaction.Item.ID, "error", err)
			return
		}
	}

	err = colls.Transactions.CraftTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.Item.ID,
		transaction.Quantity,
	)
	if err != nil {
		slog.Error("Failed creating craft transaction", "error", err)
	}
}

func handleDismantleItem(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
	var transaction struct {
		ID       bson.ObjectID `json:"_id"`
		SellerID bson.ObjectID `json:"sellerId"`
		Item     Item          `json:"item"`
		Quantity int           `json:"quantity"`
	}
	err := json.Unmarshal(t.Raw, &transaction)
	if err != nil {
		slog.Error("Failed unmarshalling wage data", "error", err)
		return
	}

	exists, err := colls.Trackers.Item.Exists(ctx, transaction.Item.ID)
	if err != nil {
		slog.Error("Failed checking if item exists", "object id", transaction.Item.ID, "error", err)
		return
	}

	if !exists {
		err = colls.Trackers.Item.Create(
			ctx,
			transaction.Item.ID,
			transaction.Item.Code,
			transaction.Item.Skills,
			transaction.SellerID,
		)
		if err != nil {
			slog.Error("Failed creating new item!", "object id", transaction.Item.ID, "error", err)
			return
		}
	}

	newEnum := enums.DISMANTLED
	if transaction.Item.State == 0 {
		newEnum = enums.BROKEN
	}

	err = colls.Trackers.Item.SetStatus(ctx, transaction.Item.ID, newEnum)
	if err != nil {
		slog.Error("Failed setting new state of item", "error", err)
		return
	}

	err = colls.Transactions.DismantleTransaction.Create(
		ctx,
		transaction.ID,
		transaction.SellerID,
		transaction.Item.ID,
		transaction.Quantity,
	)
	if err != nil {
		slog.Error("Failed creating dismantle transaction", "error", err)
	}
}

func handleBattleLoot(ctx context.Context, colls *models.Collections, t scraper.Transaction) {
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

	exists, err := colls.Trackers.Item.Exists(ctx, transaction.Item.ID)
	if err != nil {
		slog.Error("Failed checking if item exists", "object id", transaction.Item.ID, "error", err)
		return
	}

	if !exists {
		err = colls.Trackers.Item.Create(
			ctx,
			transaction.Item.ID,
			transaction.Item.Code,
			transaction.Item.Skills,
			transaction.BuyerID,
		)
		if err != nil {
			slog.Error("Failed creating new item!", "object id", transaction.Item.ID, "error", err)
			return
		}
	}

	err = colls.Transactions.LootTransaction.Create(
		ctx,
		transaction.ID,
		transaction.BuyerID,
		transaction.Item.ID,
	)
	if err != nil {
		slog.Error("Failed creating loot transaction", "error", err)
	}
}
