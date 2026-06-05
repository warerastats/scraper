package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

var transactionTypes = []string{
	"trading",
	"itemMarket",
	"wage",
	"openCase",
	"craftItem",
	"dismantleItem",
	"battleLoot",
}

type Transaction struct {
	CreatedAt       time.Time       `json:"createdAt"`
	TransactionType string          `json:"transactionType"`
	Raw             json.RawMessage `json:"-"`
}

func (t *Transaction) UnmarshalJSON(data []byte) error {
	type Alias Transaction
	var a Alias
	err := json.Unmarshal(data, &a)
	if err != nil {
		return err
	}
	*t = Transaction(a)
	t.Raw = data
	return nil
}

type transactionPage struct {
	Items      []Transaction `json:"items"`
	NextCursor *string       `json:"nextCursor,omitempty"`
}

// GetTransactions fetches all transactions newer than `till`, paginating until
// it reaches a page that crosses that boundary or runs out of results.
func (c *Client) GetTransactions(ctx context.Context, till time.Time) ([]Transaction, error) {
	var all []Transaction

	body := map[string]any{
		"limit":           100,
		"transactionType": transactionTypes,
	}

	for {
		raw, err := c.do(ctx, "transaction.getPaginatedTransactions", body, PriorityTransactions)
		if err != nil {
			return nil, fmt.Errorf("get transactions: %w", err)
		}

		var page transactionPage
		err = json.Unmarshal(raw, &page)
		if err != nil {
			return nil, fmt.Errorf("unmarshal transactions page: %w", err)
		}

		if len(page.Items) == 0 {
			break
		}

		reached := false
		for _, item := range page.Items {
			if !item.CreatedAt.After(till) {
				reached = true
				break
			}
			all = append(all, item)
		}

		if reached || len(page.Items) != 100 || page.NextCursor == nil {
			break
		}

		body["cursor"] = *page.NextCursor
	}

	return all, nil
}
