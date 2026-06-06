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

type muPayload struct {
	ID                  bson.ObjectID   `json:"_id"`
	User                *bson.ObjectID  `json:"user"`
	Region              bson.ObjectID   `json:"region"`
	Name                string          `json:"name"`
	AvatarUrl           string          `json:"avatarUrl"`
	MercenaryReputation float64         `json:"mercenaryReputation"`
	Members             []bson.ObjectID `json:"members"`
	Leveling            struct {
		Level int `json:"level"`
	} `json:"leveling"`
	ActiveUpgradeLevels struct {
		Headquarters int `json:"headquarters"`
		Dormitories  int `json:"dormitories"`
	} `json:"activeUpgradeLevels"`
}

// Mu parses a raw mu document from the gateway, emits owner / name /
// mercenary-reputation change events (seed on first track, diff on change),
// upserts the mu tracker, and maintains the disbanded flag. The owner and all
// members are queued for user backfill so the inactivity check has the data it
// needs on a later pass.
func (in *Ingester) Mu(ctx context.Context, raw json.RawMessage) {
	if len(raw) == 0 {
		slog.Debug("Empty mu payload from gateway; skipping")
		return
	}

	var p muPayload
	err := json.Unmarshal(raw, &p)
	if err != nil {
		slog.Error("Failed unmarshalling mu", "error", err)
		return
	}
	if p.ID.IsZero() {
		return
	}

	var owner bson.ObjectID
	if p.User != nil {
		owner = *p.User
	}

	candidates := make([]bson.ObjectID, 0, len(p.Members)+1)
	candidates = append(candidates, owner)
	candidates = append(candidates, p.Members...)
	in.enqueueMissingUsers(ctx, candidates)

	prev, err := in.colls.Trackers.Mu.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior mu", "muId", p.ID.Hex(), "error", err)
	}
	firstPopulated := prev == nil || prev.Name == ""

	in.emitMuEvents(ctx, p.ID, firstPopulated, prev, owner, p.Name, p.MercenaryReputation)

	err = in.colls.Trackers.Mu.UpsertMu(ctx, p.ID, trackers.Mu{
		OwnerUserID:         owner,
		RegionID:            p.Region,
		Name:                p.Name,
		NameLower:           strings.ToLower(p.Name),
		AvatarUrl:           p.AvatarUrl,
		Level:               p.Leveling.Level,
		HeadQuarterLevel:    p.ActiveUpgradeLevels.Headquarters,
		DormitoriesLevel:    p.ActiveUpgradeLevels.Dormitories,
		MercenaryReputation: p.MercenaryReputation,
		MemberUserIDs:       p.Members,
		LatestObject:        raw,
	})
	if err != nil {
		slog.Error("Failed upserting mu", "muId", p.ID.Hex(), "error", err)
		return
	}

	in.applyMuDisband(ctx, p.ID, owner, p.Members, prev)
}

func (in *Ingester) emitMuEvents(
	ctx context.Context,
	muID bson.ObjectID,
	firstPopulated bool,
	prev *trackers.Mu,
	owner bson.ObjectID,
	name string,
	mercRep float64,
) {
	logEvent := func(name string, err error) {
		if err != nil {
			slog.Error("Failed writing mu change event", "event", name, "muId", muID.Hex(), "error", err)
		}
	}

	if firstPopulated || prev.OwnerUserID != owner {
		logEvent("owner", in.colls.Events.MuOwnerChange.Set(ctx, events.MuOwnerChange{
			MuID:        muID,
			OwnerUserID: owner,
		}))
	}
	if firstPopulated || prev.Name != name {
		logEvent("name", in.colls.Events.MuNameChange.Set(ctx, events.MuNameChange{
			MuID: muID,
			Name: name,
		}))
	}
	if firstPopulated || prev.MercenaryReputation != mercRep {
		logEvent("mercRep", in.colls.Events.MuMercenaryReputationChange.Set(ctx, events.MuMercenaryReputationChange{
			MuID:                muID,
			MercenaryReputation: mercRep,
		}))
	}
}

// applyMuDisband flags a mu as disbanded when its owner and members are all
// gone, or when every named member is known and inactive. If a previously
// disbanded mu no longer meets the criteria it is revived.
func (in *Ingester) applyMuDisband(ctx context.Context, muID, owner bson.ObjectID, members []bson.ObjectID, prev *trackers.Mu) {
	disbanded := owner.IsZero() && len(members) == 0
	if !disbanded && len(members) > 0 {
		allInactive, err := in.colls.Trackers.User.MembersAllInactive(ctx, members)
		if err != nil {
			slog.Error("Failed checking mu member activity", "muId", muID.Hex(), "error", err)
		} else {
			disbanded = allInactive
		}
	}

	if disbanded {
		err := in.colls.Trackers.Mu.MarkDisbanded(ctx, muID)
		if err != nil {
			slog.Error("Failed marking mu disbanded", "muId", muID.Hex(), "error", err)
		}
		return
	}

	if prev != nil && !prev.DisbandedAt.IsZero() {
		err := in.colls.Trackers.Mu.ClearDisbanded(ctx, muID)
		if err != nil {
			slog.Error("Failed clearing mu disbanded flag", "muId", muID.Hex(), "error", err)
		}
	}
}
