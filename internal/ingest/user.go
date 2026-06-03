package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"github.com/warerastats/models/models/stores/events"
	"github.com/warerastats/models/models/stores/trackers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

func objectIDPtrEqual(a, b *bson.ObjectID) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// User parses a raw user document from the gateway and upserts the tracker.
// On a real change of one of the tracked fields (or on the first populated
// upsert for this user) the matching event-store record is also written.
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

		Skills map[string]struct {
			Level int `json:"level"`
		} `json:"skills"`

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

	skills := make(map[string]int, len(user.Skills))
	for name, s := range user.Skills {
		skills[name] = s.Level
	}

	prev, err := in.colls.Trackers.User.Get(ctx, user.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior user state", "userId", user.ID.Hex(), "error", err)
	}

	// First populated upsert: either no doc, or the placeholder created by the
	// empty-user backfill (usernameLower == "").
	firstPopulated := prev == nil || prev.UsernameLower == ""

	in.emitUserEvents(ctx, user.ID, firstPopulated, prev,
		user.Username, user.UsernameLower,
		user.CountryID, user.CompanyID, user.PartyID, user.MuID, skills)

	err = in.colls.Trackers.User.UpsertUser(ctx, user.ID, trackers.User{
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
		LastUpdated:  time.Now().UTC(),
		OnlineTime:   time.Time{},
		Wealth:       user.Stats.Wealth,
		CaseOpenings: caseOpenings,
		CountryID:    user.CountryID,
		CompanyID:    user.CompanyID,
		PartyID:      user.PartyID,
		MuID:         user.MuID,
		MilitaryRank: user.MilitaryRank,
		Skills:       skills,
		LatestObject: raw,
	})
	if err != nil {
		slog.Error("Failed upserting user data", "error", err)
	}
}

func (in *Ingester) emitUserEvents(
	ctx context.Context,
	userID bson.ObjectID,
	firstPopulated bool,
	prev *trackers.User,
	username, usernameLower string,
	countryID bson.ObjectID,
	companyID, partyID, muID *bson.ObjectID,
	skills map[string]int,
) {
	logEvent := func(name string, err error) {
		if err != nil {
			slog.Error("Failed writing user change event", "event", name, "userId", userID.Hex(), "error", err)
		}
	}

	if firstPopulated || prev.Username != username {
		logEvent("name", in.colls.Events.UserNameChange.Set(ctx, events.UserNameChange{
			UserID:        userID,
			Username:      username,
			UsernameLower: usernameLower,
		}))
	}
	if firstPopulated || prev.CountryID != countryID {
		c := countryID
		logEvent("country", in.colls.Events.UserCountryChange.Set(ctx, events.UserCountryChange{
			UserID:    userID,
			CountryID: &c,
		}))
	}
	if firstPopulated || !objectIDPtrEqual(prev.CompanyID, companyID) {
		logEvent("company", in.colls.Events.UserCompanyChange.Set(ctx, events.UserCompanyChange{
			UserID:    userID,
			CompanyID: companyID,
		}))
	}
	if firstPopulated || !objectIDPtrEqual(prev.PartyID, partyID) {
		logEvent("party", in.colls.Events.UserPartyChange.Set(ctx, events.UserPartyChange{
			UserID:  userID,
			PartyID: partyID,
		}))
	}
	if firstPopulated || !objectIDPtrEqual(prev.MuID, muID) {
		logEvent("mu", in.colls.Events.UserMUChange.Set(ctx, events.UserMUChange{
			UserID: userID,
			MUID:   muID,
		}))
	}
	if firstPopulated || !reflect.DeepEqual(prev.Skills, skills) {
		logEvent("skills", in.colls.Events.UserSkillChange.Set(ctx, events.UserSkillChange{
			UserID: userID,
			Skills: skills,
		}))
	}
}
