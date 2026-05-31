package scraper

import (
	"encoding/json"
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
	if err := json.Unmarshal(data, &a); err != nil {
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

func GetTransactions(till time.Time) ([]Transaction, error) {
	var all []Transaction

	var body = map[string]any{
		"limit":           100,
		"transactionType": transactionTypes,
	}

	for {
		raw, err := req("transaction.getPaginatedTransactions", body, 10)

		if err != nil {
			slog.Error("Failed getting transactions!", "error", err)
			return nil, err
		}

		var page transactionPage
		if err := json.Unmarshal(raw, &page); err != nil {
			slog.Error("Failed unmarshalling transactions!", "error", err)
			return nil, err
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

		body["cursor"] = page.NextCursor
	}

	return all, nil
}
