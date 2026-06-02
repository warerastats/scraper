package timer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/handlers"
	"github.com/warerastats/scraper/internal/scraper"
	"golang.org/x/sync/errgroup"
)

func Users(ctx context.Context, colls *models.Collections) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		rawUserData, err := GetUsers(ctx, colls)
		if err != nil {
			slog.Error("Failed empty user check loop", "error", err)
		} else {
			for _, rawUser := range rawUserData {
				go handlers.HandleUserData(ctx, colls, rawUser)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func GetUsers(ctx context.Context, colls *models.Collections) ([]json.RawMessage, error) {
	emptyUserIDs, err := colls.Trackers.User.GetEmpty(ctx)
	if err != nil {
		slog.Error("Failed getting empty users", "error", err)
		return nil, err
	}

	rawUsersData := make([]json.RawMessage, len(emptyUserIDs))
	g, _ := errgroup.WithContext(ctx)

	for i, ID := range emptyUserIDs {
		i, ID := i, ID

		g.Go(func() error {
			userData, err := scraper.GetUser(ID)
			if err == nil {
				rawUsersData[i] = userData
			}
			return err
		})
	}

	err = g.Wait()
	if err != nil {
		slog.Error("Failed scraping users", "error", err)
		return nil, err
	}
	return rawUsersData, nil
}
