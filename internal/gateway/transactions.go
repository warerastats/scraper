package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// maxPages caps how many pages a single GetTransactions call fetches so that
// a large checkpoint gap is caught up incrementally across many ticks rather
// than requiring thousands of sequential requests in one call.
const maxPages = 50

// GetTransactions fetches transactions newer than `till`, paginating up to
// maxPages pages. On a mid-pagination error the successfully collected
// transactions are returned alongside the error so the caller can still
// advance its checkpoint incrementally.
func (c *Client) GetTransactions(ctx context.Context, till time.Time) ([]Transaction, error) {
	var all []Transaction

	body := map[string]any{
		"limit":           100,
		"transactionType": transactionTypes,
	}

	for page := 0; page < maxPages; page++ {
		raw, err := c.do(ctx, "transaction.getPaginatedTransactions", body, PriorityTransactions)
		if err != nil {
			slog.Warn("GetTransactions page failed; returning partial results",
				"page", page, "collected", len(all), "error", err)
			return all, fmt.Errorf("get transactions page %d: %w", page, err)
		}

		var resp transactionPage
		err = json.Unmarshal(raw, &resp)
		if err != nil {
			slog.Warn("GetTransactions unmarshal failed; returning partial results",
				"page", page, "collected", len(all), "error", err)
			return all, fmt.Errorf("unmarshal transactions page %d: %w", page, err)
		}

		if len(resp.Items) == 0 {
			break
		}

		reached := false
		for _, item := range resp.Items {
			if !item.CreatedAt.After(till) {
				reached = true
				break
			}
			all = append(all, item)
		}

		if reached || len(resp.Items) != 100 || resp.NextCursor == nil {
			break
		}

		body["cursor"] = *resp.NextCursor
	}

	return all, nil
}
