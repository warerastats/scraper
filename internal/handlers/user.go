package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/trackers"
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

func derefInt(p *int) int {
	if p != nil {
		return *p
	}
	return 0
}

func latestTime(times ...time.Time) time.Time {
	var latest time.Time
	for _, t := range times {
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}

func HandleUserData(ctx context.Context, colls *models.Collections, rawUserData json.RawMessage) {
	var user struct {
		Dates struct {
			LastConnectionAt          time.Time `json:"lastConnectionAt"`
			LastCountryMessageCheckAt time.Time `json:"lastCountryMessageCheckAt"`
			LastGlobalMessageCheckAt  time.Time `json:"lastGlobalMessageCheckAt"`
			LastEventsCheckAt         time.Time `json:"lastEventsCheckAt"`
			LastWorkAt                time.Time `json:"lastWorkAt"`
			LastDailyRewardClaimedAt  time.Time `json:"lastDailyRewardClaimedAt"`
			LastHelpAskedAt           time.Time `json:"lastHelpAskedAt"`
			LastCompanyJoinedAt       time.Time `json:"lastCompanyJoinedAt"`
			LastSkillsResetAt         time.Time `json:"lastSkillsResetAt"`
		} `json:"dates"`

		Leveling struct {
			Level int `json:"level"`
		} `json:"leveling"`

		ID                bson.ObjectID `json:"_id"`
		Username          string        `json:"username"`
		UsernameLower     string        `json:"usernameLower"`
		AvatarUrl         string        `json:"avatarUrl"`
		AnimatedAvatarUrl *string       `json:"animatedAvatarUrl"`
		MilitaryRank      int           `json:"militaryRank"`

		Stats struct {
			Wealth map[string]float64 `json:"wealth"`
			Case1  *struct {
				ByRarity struct {
					Uncommon  *int `json:"uncommon,omitempty"`
					Common    *int `json:"common,omitempty"`
					Rare      *int `json:"rare,omitempty"`
					Epic      *int `json:"epic,omitempty"`
					Legendary *int `json:"legendary,omitempty"`
					Mythic    *int `json:"mythic,omitempty"`
				} `json:"byRarity"`
			} `json:"case1,omitempty"`
			Case2 *struct {
				ByRarity struct {
					Uncommon  *int `json:"uncommon,omitempty"`
					Common    *int `json:"common,omitempty"`
					Rare      *int `json:"rare,omitempty"`
					Epic      *int `json:"epic,omitempty"`
					Legendary *int `json:"legendary,omitempty"`
					Mythic    *int `json:"mythic,omitempty"`
				} `json:"byRarity"`
			} `json:"case2,omitempty"`
		} `json:"stats"`

		CountryID bson.ObjectID  `json:"country"`
		CompanyID *bson.ObjectID `json:"company,omitempty"`
		PartyID   *bson.ObjectID `json:"party,omitempty"`
		MuID      *bson.ObjectID `json:"mu,omitempty"`
	}
	err := json.Unmarshal(rawUserData, &user)
	if err != nil {
		slog.Error("Failed unmarshalling trade data", "error", err)
		return
	}

	avatar := user.AvatarUrl
	if user.AnimatedAvatarUrl != nil {
		avatar = *user.AnimatedAvatarUrl
	}

	caseOpenings := make(map[string]trackers.UserCaseStats)
	if user.Stats.Case1 != nil {
		caseOpenings["case1"] = trackers.UserCaseStats{
			Common:    derefInt(user.Stats.Case1.ByRarity.Common),
			Uncommon:  derefInt(user.Stats.Case1.ByRarity.Uncommon),
			Rare:      derefInt(user.Stats.Case1.ByRarity.Rare),
			Epic:      derefInt(user.Stats.Case1.ByRarity.Epic),
			Legendary: derefInt(user.Stats.Case1.ByRarity.Legendary),
			Mythic:    derefInt(user.Stats.Case1.ByRarity.Mythic),
		}
	}
	if user.Stats.Case2 != nil {
		caseOpenings["case2"] = trackers.UserCaseStats{
			Common:    derefInt(user.Stats.Case2.ByRarity.Common),
			Uncommon:  derefInt(user.Stats.Case2.ByRarity.Uncommon),
			Rare:      derefInt(user.Stats.Case2.ByRarity.Rare),
			Epic:      derefInt(user.Stats.Case2.ByRarity.Epic),
			Legendary: derefInt(user.Stats.Case2.ByRarity.Legendary),
			Mythic:    derefInt(user.Stats.Case2.ByRarity.Mythic),
		}
	}

	err = colls.Trackers.User.UpsertUser(ctx, user.ID, trackers.User{
		Username:      user.Username,
		UsernameLower: user.UsernameLower,
		Level:         user.Leveling.Level,
		AvatarUrl:     avatar,
		LastDate: latestTime(
			user.Dates.LastConnectionAt,
			user.Dates.LastCountryMessageCheckAt,
			user.Dates.LastGlobalMessageCheckAt,
			user.Dates.LastEventsCheckAt,
			user.Dates.LastWorkAt,
			user.Dates.LastDailyRewardClaimedAt,
			user.Dates.LastHelpAskedAt,
			user.Dates.LastCompanyJoinedAt,
			user.Dates.LastSkillsResetAt,
		),
		OnlineTime:   time.Time{},
		Wealth:       user.Stats.Wealth,
		CaseOpenings: caseOpenings,
		CountryID:    user.CountryID,
		CompanyID:    user.CompanyID,
		PartyID:      user.PartyID,
		MuID:         user.MuID,
		MilitaryRank: user.MilitaryRank,
		LatestObject: rawUserData,
	})

	if err != nil {
		slog.Error("failed upserting user data", "error", err)
	}
}
