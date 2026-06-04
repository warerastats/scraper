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

// GetUserForRefresh fetches the full user document at filler priority. The
// gateway only includes filler calls in batches that already need to flush
// for higher-priority traffic.
func (c *Client) GetUserForRefresh(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "user.getUserById", map[string]any{"userId": id.Hex()}, PriorityUserRefresh)
	if err != nil {
		return nil, fmt.Errorf("refresh user %s: %w", id.Hex(), err)
	}
	return raw, nil
}

// GetUserForDamage fetches the full user document at damage-attribution
// priority. Used by the battle-ranking pipeline when a user has dealt new
// damage and needs a fresh snapshot of equipment / skills.
func (c *Client) GetUserForDamage(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "user.getUserById", map[string]any{"userId": id.Hex()}, PriorityDamage)
	if err != nil {
		return nil, fmt.Errorf("damage-refetch user %s: %w", id.Hex(), err)
	}
	return raw, nil
}
