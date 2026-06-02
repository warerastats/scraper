package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// GetUser fetches the full user document for the given ID.
func (c *Client) GetUser(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "user.getUserById", map[string]any{"userId": id.Hex()}, PriorityUser)
	if err != nil {
		return nil, fmt.Errorf("get user %s: %w", id.Hex(), err)
	}
	return raw, nil
}
