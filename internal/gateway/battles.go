package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// GetBattleRanking fetches the cumulative damage leaderboard for one side of
// a battle. The upstream tRPC method is `battleRanking.getRanking`.
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
	}
	raw, err := c.do(ctx, "battleRanking.getRanking", body, PriorityDamage)
	if err != nil {
		return nil, fmt.Errorf("get battle ranking %s/%s: %w", battleID.Hex(), side, err)
	}
	return raw, nil
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
