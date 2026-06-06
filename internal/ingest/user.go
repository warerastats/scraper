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

// skillsMapToStruct converts the camelCase skill map from the upstream API
// into the typed UserSkills struct used by the skills snapshot store.
// Unknown keys are silently ignored.
func skillsMapToStruct(skills map[string]int) trackers.UserSkills {
	return trackers.UserSkills{
		Energy:           skills["energy"],
		Health:           skills["health"],
		Hunger:           skills["hunger"],
		Attack:           skills["attack"],
		Companies:        skills["companies"],
		Entrepreneurship: skills["entrepreneurship"],
		Production:       skills["production"],
		CriticalChance:   skills["criticalChance"],
		CriticalDamages:  skills["criticalDamages"],
		Armor:            skills["armor"],
		Precision:        skills["precision"],
		Dodge:            skills["dodge"],
		LootChance:       skills["lootChance"],
		Management:       skills["management"],
	}
}

// User parses a raw user document from the gateway and upserts the tracker.
// On a real change of one of the tracked fields (or on the first populated
// upsert for this user) the matching event-store record is also written. It
// returns the upserted tracker (nil on a parse failure) so hot callers such as
// the damage attributor can avoid an immediate re-read.
func (in *Ingester) User(ctx context.Context, raw json.RawMessage) *trackers.User {
	if len(raw) == 0 {
		slog.Debug("Empty user payload from gateway; skipping")
		return nil
	}

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

		Equipment *struct {
			GlovesItemID *bson.ObjectID `json:"gloves,omitempty"`
			HelmetItemID *bson.ObjectID `json:"helmet,omitempty"`
			ChestItemID  *bson.ObjectID `json:"chest,omitempty"`
			PantsItemID  *bson.ObjectID `json:"pants,omitempty"`
			BootsItemID  *bson.ObjectID `json:"boots,omitempty"`
			Ammo         *string        `json:"ammo,omitempty"`
			WeaponItemID *bson.ObjectID `json:"weapon,omitempty"`
		} `json:"equipment,omitempty"`

		Skills map[string]struct {
			Level int `json:"level"`
		} `json:"skills"`

		CountryID bson.ObjectID  `json:"country"`
		CompanyID *bson.ObjectID `json:"company,omitempty"`
		PartyID   *bson.ObjectID `json:"party,omitempty"`
		MuID      *bson.ObjectID `json:"mu,omitempty"`
	}

	err := json.Unmarshal(raw, &user)
	if err != nil {
		slog.Error("Failed unmarshalling user data", "error", err)
		return nil
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

	tracker := trackers.User{
		ID:            user.ID,
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
	}
	err = in.colls.Trackers.User.UpsertUser(ctx, user.ID, tracker)
	if err != nil {
		slog.Error("Failed upserting user data", "error", err)
	}

	in.snapshotUserSkills(ctx, user.ID, skills)

	// A user referencing a mu or party both proves the entity exists (queue a
	// placeholder if we don't track it yet) and counts as activity that keeps
	// the entity "fresh" for refresh / revives it from a disbanded state.
	if user.MuID != nil && !user.MuID.IsZero() {
		in.muQueue.Enqueue(*user.MuID)
		in.muLastSeen.Mark(*user.MuID)
	}
	if user.PartyID != nil && !user.PartyID.IsZero() {
		in.partyQueue.Enqueue(*user.PartyID)
		in.partyLastSeen.Mark(*user.PartyID)
	}

	if user.Equipment != nil {
		ids := []*bson.ObjectID{
			user.Equipment.WeaponItemID,
			user.Equipment.HelmetItemID,
			user.Equipment.ChestItemID,
			user.Equipment.PantsItemID,
			user.Equipment.BootsItemID,
			user.Equipment.GlovesItemID,
		}
		var missing []bson.ObjectID
		for _, p := range ids {
			if p == nil || p.IsZero() {
				continue
			}
			exists, err := in.colls.Trackers.Item.Exists(ctx, *p)
			if err != nil {
				slog.Error("Failed checking equipment item existence", "userId", user.ID.Hex(), "itemId", p.Hex(), "error", err)
				continue
			}
			if !exists {
				missing = append(missing, *p)
			}
		}
		if len(missing) > 0 {
			err = in.colls.Trackers.Item.CreateEmpty(ctx, missing)
			if err != nil {
				slog.Error("Failed creating equipment item placeholders", "userId", user.ID.Hex(), "count", len(missing), "error", err)
			}
		}
	}

	return &tracker
}

// snapshotUserSkills compares the user's latest stored skill snapshot with
// the freshly-ingested values. If there's no prior snapshot, or if any field
// differs, a new snapshot is appended to the skills collection.
func (in *Ingester) snapshotUserSkills(ctx context.Context, userID bson.ObjectID, skills map[string]int) {
	current := skillsMapToStruct(skills)
	prev, err := in.colls.Trackers.Skill.GetLatestForUser(ctx, userID)
	if err != nil {
		slog.Error("Failed loading latest skill snapshot", "userId", userID.Hex(), "error", err)
		return
	}
	if prev != nil && prev.Skills == current {
		return
	}
	_, err = in.colls.Trackers.Skill.Create(ctx, userID, current, time.Now().UTC())
	if err != nil {
		slog.Error("Failed inserting skill snapshot", "userId", userID.Hex(), "error", err)
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
