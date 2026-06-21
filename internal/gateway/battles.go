package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// rankingPage is the paginated response shape from battleRanking.getRanking.
type rankingPage struct {
	Items      []json.RawMessage `json:"items"`
	NextCursor *string           `json:"nextCursor"`
}

// maxRankingPages caps pagination loops to prevent runaway requests.
const maxRankingPages = 20

// GetBattleRanking fetches the full cumulative damage leaderboard for one side
// of a battle, paginating through all pages. Returns a bare JSON array of
// ranking entries.
func (c *Client) GetBattleRanking(
	ctx context.Context,
	battleID bson.ObjectID,
	side enums.Side,
) (json.RawMessage, error) {
	body := map[string]any{
		"dataType": "damage",
		"type":     "user",
		"side":     sideToWire(side),
		"battleId": battleID.Hex(),
		"limit":    100,
	}

	var all []json.RawMessage
	for page := 0; page < maxRankingPages; page++ {
		raw, err := c.do(ctx, "battleRanking.getRanking", body, PriorityDamage)
		if err != nil {
			return nil, fmt.Errorf("get battle ranking %s/%s page %d: %w", battleID.Hex(), side, page, err)
		}

		var p rankingPage
		if err = json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("unmarshal ranking page %s/%s: %w", battleID.Hex(), side, err)
		}
		all = append(all, p.Items...)

		if p.NextCursor == nil || *p.NextCursor == "" {
			break
		}
		body["cursor"] = *p.NextCursor
	}

	out, err := json.Marshal(all)
	if err != nil {
		return nil, fmt.Errorf("marshal combined ranking %s/%s: %w", battleID.Hex(), side, err)
	}
	return out, nil
}

// GetBattleByID fetches the full battle object from `battle.getById`. Used to
// finalise a battle once it transitions to inactive.
func (c *Client) GetBattleByID(
	ctx context.Context,
	battleID bson.ObjectID,
) (json.RawMessage, error) {
	body := map[string]any{"battleId": battleID.Hex()}
	raw, err := c.do(ctx, "battle.getById", body, PriorityDamage)
	if err != nil {
		return nil, fmt.Errorf("get battle %s: %w", battleID.Hex(), err)
	}
	return raw, nil
}

// sideToWire converts a typed enums.Side to the lowercase string the upstream
// API expects ("attacker"/"defender").
func sideToWire(side enums.Side) string {
	switch side {
	case enums.ATTACKER:
		return "attacker"
	case enums.DEFENDER:
		return "defender"
	default:
		return string(side)
	}
}
