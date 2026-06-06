package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// GetParty fetches the full party document for the given ID at backfill
// priority.
func (c *Client) GetParty(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "party.getById", map[string]any{"partyId": id.Hex()}, PriorityParty)
	if err != nil {
		return nil, fmt.Errorf("get party %s: %w", id.Hex(), err)
	}
	return raw, nil
}

// GetPartyForRefresh fetches the full party document at filler priority. The
// gateway only includes filler calls in batches that already need to flush for
// higher-priority traffic.
func (c *Client) GetPartyForRefresh(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "party.getById", map[string]any{"partyId": id.Hex()}, PriorityPartyRefresh)
	if err != nil {
		return nil, fmt.Errorf("refresh party %s: %w", id.Hex(), err)
	}
	return raw, nil
}

// GetPartyForRuling fetches the full party document at the elevated ruling
// priority used to guarantee every country's ruling party is re-fetched on a
// fixed cadence.
func (c *Client) GetPartyForRuling(ctx context.Context, id bson.ObjectID) (json.RawMessage, error) {
	raw, err := c.do(ctx, "party.getById", map[string]any{"partyId": id.Hex()}, PriorityRulingParty)
	if err != nil {
		return nil, fmt.Errorf("ruling party %s: %w", id.Hex(), err)
	}
	return raw, nil
}
