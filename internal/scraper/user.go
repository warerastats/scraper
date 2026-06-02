package scraper

import (
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func GetUser(ID bson.ObjectID) (json.RawMessage, error) {
	var body = map[string]any{
		"userId": ID.Hex(),
	}

	raw, err := req("user.getUserById", body, 50)
	if err != nil {
		slog.Error("Failed making request through the gateway", "error", err)
	}

	return raw, err
}
