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

// GetUserCompanies fetches the list of company IDs owned by a user.
func (c *Client) GetUserCompanies(ctx context.Context, userID bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "company.getCompanies", map[string]any{"userId": userID.Hex(), "perPage": 100}, PriorityCompanyOwnership)
	if err != nil {
		return nil, fmt.Errorf("get user companies %s: %w", userID.Hex(), err)
	}
	return raw, nil
}
