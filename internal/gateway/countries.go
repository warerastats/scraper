package gateway

import (
	"context"
	"encoding/json"
	"fmt"
)

// GetCountries fetches the full list of countries from the upstream API.
// The response is the raw `result.data` array of country objects.
func (c *Client) GetCountries(ctx context.Context) (json.RawMessage, error) {
	raw, err := c.do(ctx, "country.getAllCountries", nil, PriorityCountries)
	if err != nil {
		return nil, fmt.Errorf("get countries: %w", err)
	}
	return raw, nil
}
