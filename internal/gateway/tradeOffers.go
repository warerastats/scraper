package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TopOrder mirrors a single buy/sell order returned by the
// `tradingOrder.getTopOrders` endpoint. `Quantity` is the *remaining*
// amount on the order at the moment of the snapshot.
type TopOrder struct {
	ID        bson.ObjectID  `json:"_id"`
	UserID    bson.ObjectID  `json:"user"`
	CountryID *bson.ObjectID `json:"country,omitempty"`
	MuID      *bson.ObjectID `json:"mu,omitempty"`
	ItemCode  string         `json:"itemCode"`
	Quantity  int            `json:"quantity"`
	Price     float64        `json:"price"`
	OfferAt   time.Time      `json:"offerAt"`
	Type      string         `json:"type"`
}

type topOrdersResponse struct {
	BuyOrders  []TopOrder `json:"buyOrders"`
	SellOrders []TopOrder `json:"sellOrders"`
}

// GetTopOrders fetches the top buy and sell orders for the given itemCode.
// The upstream endpoint returns up to `limit` of each side, ordered best
// first (highest bid for buy, lowest ask for sell).
func (c *Client) GetTopOrders(ctx context.Context, itemCode string, limit int) ([]TopOrder, []TopOrder, error) {
	body := map[string]any{
		"itemCode": itemCode,
		"limit":    limit,
	}
	raw, err := c.do(ctx, "tradingOrder.getTopOrders", body, PriorityTradeOffers)
	if err != nil {
		return nil, nil, fmt.Errorf("get top orders %s: %w", itemCode, err)
	}

	var resp topOrdersResponse
	err = json.Unmarshal(raw, &resp)
	if err != nil {
		return nil, nil, fmt.Errorf("unmarshal top orders %s: %w", itemCode, err)
	}
	return resp.BuyOrders, resp.SellOrders, nil
}
