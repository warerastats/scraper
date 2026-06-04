package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// GetUserEquipment fetches the user's currently equipped items via
// `inventory.fetchCurrentEquipment`. Used as a fallback when an item ID
// referenced by a user payload has no corresponding tracker document yet.
func (c *Client) GetUserEquipment(
	ctx context.Context,
	userID bson.ObjectID,
) (json.RawMessage, error) {
	body := map[string]any{"userId": userID.Hex()}
	raw, err := c.do(ctx, "inventory.fetchCurrentEquipment", body, PriorityDamage)
	if err != nil {
		return nil, fmt.Errorf("get user equipment %s: %w", userID.Hex(), err)
	}
	return raw, nil
}
