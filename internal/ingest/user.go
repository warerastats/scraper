package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/stores/trackers"
	"go.mongodb.org/mongo-driver/v2/bson"
)

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

func caseStatsFrom(stats *CaseStats) trackers.UserCaseStats {
	if stats == nil {
		return trackers.UserCaseStats{}
	}
	return trackers.UserCaseStats{
		Common:    derefInt(stats.ByRarity.Common),
		Uncommon:  derefInt(stats.ByRarity.Uncommon),
		Rare:      derefInt(stats.ByRarity.Rare),
		Epic:      derefInt(stats.ByRarity.Epic),
		Legendary: derefInt(stats.ByRarity.Legendary),
		Mythic:    derefInt(stats.ByRarity.Mythic),
	}
}

// User parses a raw user document from the gateway and upserts the tracker.
func (in *Ingester) User(ctx context.Context, raw json.RawMessage) {
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
			Case1  *CaseStats         `json:"case1,omitempty"`
			Case2  *CaseStats         `json:"case2,omitempty"`
		} `json:"stats"`

		CountryID bson.ObjectID  `json:"country"`
		CompanyID *bson.ObjectID `json:"company,omitempty"`
		PartyID   *bson.ObjectID `json:"party,omitempty"`
		MuID      *bson.ObjectID `json:"mu,omitempty"`
	}
	if err := json.Unmarshal(raw, &user); err != nil {
		slog.Error("Failed unmarshalling user data", "error", err)
		return
	}

	avatar := user.AvatarUrl
	if user.AnimatedAvatarUrl != nil {
		avatar = *user.AnimatedAvatarUrl
	}

	caseOpenings := make(map[string]trackers.UserCaseStats)
	if user.Stats.Case1 != nil {
		caseOpenings["case1"] = caseStatsFrom(user.Stats.Case1)
	}
	if user.Stats.Case2 != nil {
		caseOpenings["case2"] = caseStatsFrom(user.Stats.Case2)
	}

	err := in.colls.Trackers.User.UpsertUser(ctx, user.ID, trackers.User{
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
		LatestObject: raw,
	})
	if err != nil {
		slog.Error("Failed upserting user data", "error", err)
	}
}
