package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// GetCompany fetches the full company document for the given ID.
func (c *Client) GetCompany(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "company.getById", map[string]any{"companyId": id.Hex()}, PriorityCompanies)
	if err != nil {
		return nil, fmt.Errorf("get company %s: %w", id.Hex(), err)
	}
	return raw, nil
}

// GetWorkers fetches the list of workers currently employed at the given
// company. The shape is {type, workers: [...]}.
func (c *Client) GetWorkers(ctx context.Context, companyID bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "worker.getWorkers", map[string]any{"companyId": companyID.Hex()}, PriorityCompanies)
	if err != nil {
		return nil, fmt.Errorf("get workers %s: %w", companyID.Hex(), err)
	}
	return raw, nil
}
