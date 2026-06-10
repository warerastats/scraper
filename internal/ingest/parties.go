package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"

	"github.com/warerastats/models/models/stores/events"
	"github.com/warerastats/models/models/stores/trackers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type partyPayload struct {
	ID             bson.ObjectID   `json:"_id"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Country        bson.ObjectID   `json:"country"`
	Region         bson.ObjectID   `json:"region"`
	Leader         bson.ObjectID   `json:"leader"`
	CouncilMembers []bson.ObjectID `json:"councilMembers"`
	Members        []bson.ObjectID `json:"members"`
	AvatarUrl      string          `json:"avatarUrl"`
	Ethics         struct {
		Unethical     bool `json:"unethical"`
		Militarism    int  `json:"militarism"`
		Isolationism  int  `json:"isolationism"`
		Imperialism   int  `json:"imperialism"`
		Industrialism int  `json:"industrialism"`
	} `json:"ethics"`
}

// Party parses a raw party document from the gateway, emits name / description
// / leader / ethics change events (seed on first track, diff on change),
// upserts the party tracker, and maintains the disbanded flag. The leader and
// all members are queued for user backfill so the inactivity check has the data
// it needs on a later pass.
func (in *Ingester) Party(ctx context.Context, raw json.RawMessage) {
	if len(raw) == 0 {
		slog.Debug("Empty party payload from gateway; skipping")
		return
	}

	var p partyPayload
	err := json.Unmarshal(raw, &p)
	if err != nil {
		slog.Error("Failed unmarshalling party", "error", err)
		return
	}
	if p.ID.IsZero() {
		return
	}

	candidates := make([]bson.ObjectID, 0, len(p.Members)+1)
	candidates = append(candidates, p.Leader)
	candidates = append(candidates, p.Members...)
	in.enqueueMissingUsers(ctx, candidates)

	prev, err := in.colls.Trackers.Party.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior party", "partyId", p.ID.Hex(), "error", err)
	}
	firstPopulated := prev == nil || prev.Name == ""

	in.emitPartyEvents(ctx, p, firstPopulated, prev)

	var party trackers.Party
	party.Name = p.Name
	party.NameLower = strings.ToLower(p.Name)
	party.Description = p.Description
	party.CountryID = p.Country
	party.RegionID = p.Region
	party.LeaderUserID = p.Leader
	party.MemberUserIDs = p.Members
	party.AvatarUrl = p.AvatarUrl
	party.Ethics.Unethical = p.Ethics.Unethical
	party.Ethics.Militarism = p.Ethics.Militarism
	party.Ethics.Isolationism = p.Ethics.Isolationism
	party.Ethics.Imperialism = p.Ethics.Imperialism
	party.Ethics.Industrialism = p.Ethics.Industrialism
	party.LatestObject = raw

	err = in.colls.Trackers.Party.UpsertParty(ctx, p.ID, party)
	if err != nil {
		slog.Error("Failed upserting party", "partyId", p.ID.Hex(), "error", err)
		return
	}

	in.applyPartyDisband(ctx, p.ID, p.Members, prev)
}

func (in *Ingester) emitPartyEvents(ctx context.Context, p partyPayload, firstPopulated bool, prev *trackers.Party) {
	logEvent := func(name string, err error) {
		if err != nil {
			slog.Error("Failed writing party change event", "event", name, "partyId", p.ID.Hex(), "error", err)
		}
	}

	if firstPopulated || prev.Name != p.Name {
		logEvent("name", in.colls.Events.PartyNameChange.Set(ctx, events.PartyNameChange{
			PartyID: p.ID,
			Name:    p.Name,
		}))
	}
	if firstPopulated || prev.Description != p.Description {
		logEvent("description", in.colls.Events.PartyDescriptionChange.Set(ctx, events.PartyDescriptionChange{
			PartyID:     p.ID,
			Description: p.Description,
		}))
	}
	if firstPopulated || prev.LeaderUserID != p.Leader {
		logEvent("leader", in.colls.Events.PartyLeaderChange.Set(ctx, events.PartyLeaderChange{
			PartyID:      p.ID,
			LeaderUserID: p.Leader,
		}))
	}
	ethicsChanged := firstPopulated ||
		prev.Ethics.Unethical != p.Ethics.Unethical ||
		prev.Ethics.Militarism != p.Ethics.Militarism ||
		prev.Ethics.Isolationism != p.Ethics.Isolationism ||
		prev.Ethics.Imperialism != p.Ethics.Imperialism ||
		prev.Ethics.Industrialism != p.Ethics.Industrialism
	if ethicsChanged {
		change := events.PartyEthicsChange{PartyID: p.ID}
		change.Ethics.Unethical = p.Ethics.Unethical
		change.Ethics.Militarism = p.Ethics.Militarism
		change.Ethics.Isolationism = p.Ethics.Isolationism
		change.Ethics.Imperialism = p.Ethics.Imperialism
		change.Ethics.Industrialism = p.Ethics.Industrialism
		logEvent("ethics", in.colls.Events.PartyEthicsChange.Set(ctx, change))
	}
}

// applyPartyDisband flags a party as disbanded when it has no members, or when
// every named member is known and inactive. If a previously disbanded party no
// longer meets the criteria it is revived.
func (in *Ingester) applyPartyDisband(ctx context.Context, partyID bson.ObjectID, members []bson.ObjectID, prev *trackers.Party) {
	disbanded := len(members) == 0
	if !disbanded {
		allInactive, err := in.colls.Trackers.User.MembersAllInactive(ctx, members)
		if err != nil {
			slog.Error("Failed checking party member activity", "partyId", partyID.Hex(), "error", err)
		} else {
			disbanded = allInactive
		}
	}

	if disbanded {
		err := in.colls.Trackers.Party.MarkDisbanded(ctx, partyID)
		if err != nil {
			slog.Error("Failed marking party disbanded", "partyId", partyID.Hex(), "error", err)
		}
		return
	}

	if prev != nil && !prev.DisbandedAt.IsZero() {
		err := in.colls.Trackers.Party.ClearDisbanded(ctx, partyID)
		if err != nil {
			slog.Error("Failed clearing party disbanded flag", "partyId", partyID.Hex(), "error", err)
		}
	}
}
