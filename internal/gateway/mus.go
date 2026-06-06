package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// GetMu fetches the full mu document for the given ID at backfill priority.
func (c *Client) GetMu(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "mu.getById", map[string]any{"muId": id.Hex()}, PriorityMu)
	if err != nil {
		return nil, fmt.Errorf("get mu %s: %w", id.Hex(), err)
	}
	return raw, nil
}

// GetMuForRefresh fetches the full mu document at filler priority. The gateway
// only includes filler calls in batches that already need to flush for
// higher-priority traffic.
func (c *Client) GetMuForRefresh(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "mu.getById", map[string]any{"muId": id.Hex()}, PriorityMuRefresh)
	if err != nil {
		return nil, fmt.Errorf("refresh mu %s: %w", id.Hex(), err)
	}
	return raw, nil
}
