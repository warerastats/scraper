package gateway

import (
	"context"
	"encoding/json"
	"fmt"
)

// GetRegions fetches the full regions object from the upstream API. The
// response is the raw `result.data` map keyed by region ID.
func (c *Client) GetRegions(ctx context.Context) (json.RawMessage, error) {
	raw, err := c.do(ctx, "region.getRegionsObject", nil, PriorityRegions)
	if err != nil {
		return nil, fmt.Errorf("get regions: %w", err)
	}
	return raw, nil
}
