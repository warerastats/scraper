package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/stores/events"
	"github.com/warerastats/models/models/stores/trackers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type regionDeposit struct {
	Type         string    `json:"type"`
	StartsAt     time.Time `json:"startsAt"`
	EndsAt       time.Time `json:"endsAt"`
	BonusPercent float64   `json:"bonusPercent"`
}

type regionPayload struct {
	ID                bson.ObjectID   `json:"_id"`
	Name              string          `json:"name"`
	Country           bson.ObjectID   `json:"country"`
	InitialCountry    bson.ObjectID   `json:"initialCountry"`
	Neighbors         []bson.ObjectID `json:"neighbors"`
	IsCapital         bool            `json:"isCapital"`
	IsLinkedToCapital bool            `json:"isLinkedToCapital"`
	Resistance        float64         `json:"resistance"`
	ResistanceMax     float64         `json:"resistanceMax"`
	Deposit           *regionDeposit  `json:"deposit,omitempty"`
	StrategicResource *string         `json:"strategicResource,omitempty"`
	ActiveBattle      json.RawMessage `json:"activeBattle,omitempty"`
}

type battleSide struct {
	Country       bson.ObjectID   `json:"country"`
	Region        *bson.ObjectID  `json:"region,omitempty"`
	CountryOrders []bson.ObjectID `json:"countryOrders"`
	MuOrders      []bson.ObjectID `json:"muOrders"`
}

type battlePayload struct {
	ID       bson.ObjectID `json:"_id"`
	IsActive bool          `json:"isActive"`
	Attacker battleSide    `json:"attacker"`
	Defender battleSide    `json:"defender"`
}

// Regions parses the full /region.getRegionsObject payload, upserts each
// region tracker, emits region/owner/deposit/strategic-resource events on
// change, and ingests any active battles. Battles that were active in the
// DB but are no longer present in the response are flipped to inactive.
func (in *Ingester) Regions(ctx context.Context, raw json.RawMessage) {
	var byID map[string]json.RawMessage
	err := json.Unmarshal(raw, &byID)
	if err != nil {
		slog.Error("Failed unmarshalling regions object", "error", err)
		return
	}

	seenBattles := make(map[bson.ObjectID]struct{}, len(byID))

	for _, regionRaw := range byID {
		var p regionPayload
		err := json.Unmarshal(regionRaw, &p)
		if err != nil {
			slog.Error("Failed unmarshalling region", "error", err)
			continue
		}
		if p.ID.IsZero() {
			continue
		}

		in.emitRegionEvents(ctx, p)

		err = in.colls.Trackers.Region.UpsertRegion(ctx, p.ID, trackers.Region{
			Name:              p.Name,
			CountryID:         p.Country,
			InitialCountryID:  p.InitialCountry,
			NeighborRegionIDs: p.Neighbors,
			IsCapital:         p.IsCapital,
			IsLinkedToCapital: p.IsLinkedToCapital,
			Resistance:        p.Resistance,
			MaxResistance:     p.ResistanceMax,
			LatestObject:      regionRaw,
		})
		if err != nil {
			slog.Error("Failed upserting region", "regionId", p.ID.Hex(), "error", err)
		}

		if len(p.ActiveBattle) > 0 && string(p.ActiveBattle) != "null" {
			battleID, ok := in.ingestBattle(ctx, p.ID, p.Country, p.ActiveBattle)
			if ok {
				seenBattles[battleID] = struct{}{}
			}
		}
	}

	in.reconcileInactiveBattles(ctx, seenBattles)
}

func (in *Ingester) emitRegionEvents(ctx context.Context, p regionPayload) {
	logEvent := func(name string, err error) {
		if err != nil {
			slog.Error("Failed writing region change event", "event", name, "regionId", p.ID.Hex(), "error", err)
		}
	}

	// Owner
	prevOwner, err := in.colls.Events.RegionOwnerChange.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior region owner", "regionId", p.ID.Hex(), "error", err)
	}
	if prevOwner == nil || prevOwner.CountryID != p.Country {
		logEvent("owner", in.colls.Events.RegionOwnerChange.Set(ctx, events.RegionOwnerChange{
			RegionID:  p.ID,
			CountryID: p.Country,
		}))
	}

	// Deposit
	prevDep, err := in.colls.Events.RegionDeposit.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior region deposit", "regionId", p.ID.Hex(), "error", err)
	}
	var curDepType *string
	if p.Deposit != nil {
		t := p.Deposit.Type
		curDepType = &t
	}
	prevDepType := (*string)(nil)
	if prevDep != nil {
		prevDepType = prevDep.Type
	}
	if !stringPtrEqual(curDepType, prevDepType) {
		change := events.RegionDeposit{
			RegionID: p.ID,
			Type:     curDepType,
		}
		if p.Deposit != nil {
			s, e, b := p.Deposit.StartsAt, p.Deposit.EndsAt, p.Deposit.BonusPercent
			change.StartsAt = &s
			change.EndsAt = &e
			change.BonusPercent = &b
		}
		logEvent("deposit", in.colls.Events.RegionDeposit.Set(ctx, change))
	}

	// Strategic resource
	prevSR, err := in.colls.Events.RegionStrategicResource.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior region strategic resource", "regionId", p.ID.Hex(), "error", err)
	}
	prevSRVal := (*string)(nil)
	if prevSR != nil {
		prevSRVal = prevSR.Resource
	}
	if !stringPtrEqual(p.StrategicResource, prevSRVal) {
		logEvent("strategicResource", in.colls.Events.RegionStrategicResource.Set(ctx, events.RegionStrategicResource{
			RegionID: p.ID,
			Resource: p.StrategicResource,
		}))
	}
}

func (in *Ingester) ingestBattle(
	ctx context.Context,
	defenderRegionID bson.ObjectID,
	defenderCountryID bson.ObjectID,
	rawBattle json.RawMessage,
) (bson.ObjectID, bool) {
	var b battlePayload
	err := json.Unmarshal(rawBattle, &b)
	if err != nil {
		slog.Error("Failed unmarshalling battle", "regionId", defenderRegionID.Hex(), "error", err)
		return bson.ObjectID{}, false
	}
	if b.ID.IsZero() {
		return bson.ObjectID{}, false
	}

	prev, err := in.colls.Trackers.Battle.Get(ctx, b.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior battle", "battleId", b.ID.Hex(), "error", err)
	}

	in.emitBattleOrderEvents(ctx, b, prev)

	var attackerDamages, defenderDamages int
	if prev != nil {
		attackerDamages = prev.AttackerDamages
		defenderDamages = prev.DefenderDamages
	}

	now := time.Now().UTC()
	err = in.colls.Trackers.Battle.UpsertBattle(ctx, b.ID, trackers.Battle{
		AttackerRegionID:  b.Attacker.Region,
		AttackerCountryID: b.Attacker.Country,
		AttackerDamages:   attackerDamages,
		DefenderRegionID:  defenderRegionID,
		DefenderCountryID: defenderCountryID,
		DefenderDamages:   defenderDamages,
		WinnerSide:        nil,
		IsActive:          true,
		LastUpdated:       now,
		LatestObject:      rawBattle,
	})
	if err != nil {
		slog.Error("Failed upserting battle", "battleId", b.ID.Hex(), "error", err)
	}
	return b.ID, true
}

func (in *Ingester) emitBattleOrderEvents(ctx context.Context, cur battlePayload, prev *trackers.Battle) {
	var prevAttacker, prevDefender battleSide
	firstSight := prev == nil || len(prev.LatestObject) == 0

	if !firstSight {
		var prevPayload battlePayload
		err := json.Unmarshal(prev.LatestObject, &prevPayload)
		if err != nil {
			slog.Error("Failed unmarshalling prior battle raw", "battleId", cur.ID.Hex(), "error", err)
			firstSight = true
		} else {
			prevAttacker = prevPayload.Attacker
			prevDefender = prevPayload.Defender
		}
	}

	in.diffOrders(ctx, cur.ID, "attacker", "country", prevAttacker.CountryOrders, cur.Attacker.CountryOrders, firstSight)
	in.diffOrders(ctx, cur.ID, "attacker", "mu", prevAttacker.MuOrders, cur.Attacker.MuOrders, firstSight)
	in.diffOrders(ctx, cur.ID, "defender", "country", prevDefender.CountryOrders, cur.Defender.CountryOrders, firstSight)
	in.diffOrders(ctx, cur.ID, "defender", "mu", prevDefender.MuOrders, cur.Defender.MuOrders, firstSight)
}

func (in *Ingester) diffOrders(
	ctx context.Context,
	battleID bson.ObjectID,
	side, kind string,
	prev, cur []bson.ObjectID,
	firstSight bool,
) {
	prevSet := make(map[bson.ObjectID]struct{}, len(prev))
	if !firstSight {
		for _, id := range prev {
			prevSet[id] = struct{}{}
		}
	}
	curSet := make(map[bson.ObjectID]struct{}, len(cur))
	for _, id := range cur {
		curSet[id] = struct{}{}
	}

	now := time.Now().UTC()
	logEvent := func(err error) {
		if err != nil {
			slog.Error("Failed writing battle order event", "battleId", battleID.Hex(), "side", side, "kind", kind, "error", err)
		}
	}

	// Added: in cur but not prev (or first sight = all current)
	for _, id := range cur {
		if _, ok := prevSet[id]; ok {
			continue
		}
		logEvent(in.colls.Events.BattleOrderChange.Set(ctx, events.BattleOrderChange{
			BattleID: battleID,
			Side:     side,
			Kind:     kind,
			Action:   "added",
			EntityID: id,
			At:       now,
		}))
	}

	// Removed: in prev but not cur. Skipped on first sight
	if firstSight {
		return
	}
	for _, id := range prev {
		if _, ok := curSet[id]; ok {
			continue
		}
		logEvent(in.colls.Events.BattleOrderChange.Set(ctx, events.BattleOrderChange{
			BattleID: battleID,
			Side:     side,
			Kind:     kind,
			Action:   "removed",
			EntityID: id,
			At:       now,
		}))
	}
}

func (in *Ingester) reconcileInactiveBattles(ctx context.Context, seen map[bson.ObjectID]struct{}) {
	active, err := in.colls.Trackers.Battle.GetActiveIDs(ctx)
	if err != nil {
		slog.Error("Failed loading active battles for reconciliation", "error", err)
		return
	}
	var missing []bson.ObjectID
	for _, id := range active {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return
	}
	err = in.colls.Trackers.Battle.MarkInactive(ctx, missing)
	if err != nil {
		slog.Error("Failed marking battles inactive", "count", len(missing), "error", err)
		return
	}
	slog.Info("Marked battles inactive", "count", len(missing))
}

func stringPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
