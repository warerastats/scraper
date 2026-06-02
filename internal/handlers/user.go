package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var userExistsChan = make(chan bson.ObjectID, 1000)

func UserExists(userID bson.ObjectID) {
	userExistsChan <- userID
}

// HandleUsersExistsLoop starts a loop that consumes a channel with user id's
// once every second it consumes the full channel and checks if the user id's exists
// if not exists, starts go routine to grab the user data
func HandleUsersExistsLoop(ctx context.Context, colls *models.Collections) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var userIDs []bson.ObjectID
		for len(userExistsChan) > 0 {
			userIDs = append(userIDs, <-userExistsChan)
		}
		if len(userIDs) == 0 {
			continue
		}

		nonExisting, err := colls.Trackers.User.Exists(ctx, userIDs)
		if err != nil {
			slog.Error("Failed checking if user IDs exist!", "error", err)
			continue
		}

		for _, userID := range nonExisting {
			go colls.Trackers.User.CreateEmpty(ctx, userID)
		}
	}
}
