package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/warerastats/models/models/enums"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/scraper/internal/gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"
)

type equipmentSlot int

const (
	slotUnknown equipmentSlot = iota
	slotWeapon
	slotHelmet
	slotChest
	slotPants
	slotBoots
	slotGloves
)

type loadout struct {
	Weapon *bson.ObjectID
	Helmet *bson.ObjectID
	Chest  *bson.ObjectID
	Pants  *bson.ObjectID
	Boots  *bson.ObjectID
	Gloves *bson.ObjectID
	Ammo   *string
}

type rankingEntry struct {
	UserID bson.ObjectID `json:"user"`
	Value  int           `json:"value"`
	Rank   int           `json:"rank"`
}

type rankingResponse struct {
	Rankings []rankingEntry `json:"rankings"`
}

type userEquipmentPayload struct {
	Gloves *bson.ObjectID `json:"gloves,omitempty"`
	Helmet *bson.ObjectID `json:"helmet,omitempty"`
	Chest  *bson.ObjectID `json:"chest,omitempty"`
	Pants  *bson.ObjectID `json:"pants,omitempty"`
	Boots  *bson.ObjectID `json:"boots,omitempty"`
	Ammo   *string        `json:"ammo,omitempty"`
	Weapon *bson.ObjectID `json:"weapon,omitempty"`
}

type userRawForDamage struct {
	Equipment *userEquipmentPayload `json:"equipment,omitempty"`
}

type equipmentItem struct {
	ID     bson.ObjectID      `json:"_id"`
	Code   string             `json:"code"`
	Skills map[string]float64 `json:"skills"`
	State  int                `json:"state"`
}

type equipmentResponse struct {
	Weapon *equipmentItem `json:"weapon,omitempty"`
	Helmet *equipmentItem `json:"helmet,omitempty"`
	Chest  *equipmentItem `json:"chest,omitempty"`
	Pants  *equipmentItem `json:"pants,omitempty"`
	Boots  *equipmentItem `json:"boots,omitempty"`
	Gloves *equipmentItem `json:"gloves,omitempty"`
	Ammo   *string        `json:"ammo,omitempty"`
}

// BattleRanking ingests one side's leaderboard from
// `/battleRanking.getRanking`. For every entry whose stored cumulative
// damage is below the upstream value, a new Damage event is recorded for
// the delta. `prevPollAt` bounds the dismantle-history window used for
// equipment attribution.
func (in *Ingester) BattleRanking(
	ctx context.Context,
	battleID bson.ObjectID,
	side enums.Side,
	raw json.RawMessage,
	prevPollAt time.Time,
	client *gateway.Client,
) {
	var resp rankingResponse
	err := json.Unmarshal(raw, &resp)
	if err != nil {
		slog.Error("Failed unmarshalling battle ranking", "battleId", battleID.Hex(), "side", side, "error", err)
		return
	}

	for _, entry := range resp.Rankings {
		in.processRankingEntry(ctx, battleID, side, entry, prevPollAt, client)
	}

	_ = in.batchers.Damage.Flush(ctx)
}

func (in *Ingester) processRankingEntry(
	ctx context.Context,
	battleID bson.ObjectID,
	side enums.Side,
	entry rankingEntry,
	prevPollAt time.Time,
	client *gateway.Client,
) {
	if entry.UserID.IsZero() {
		return
	}

	stored, err := in.colls.Trackers.Damage.GetUserBattleTotal(ctx, battleID, entry.UserID, side)
	if err != nil {
		slog.Error("Failed reading stored damage total",
			"battleId", battleID.Hex(), "userId", entry.UserID.Hex(), "error", err)
		return
	}

	delta := entry.Value - stored
	if delta == 0 {
		return
	}
	if delta < 0 {
		slog.Warn("Negative damage delta from ranking",
			"battleId", battleID.Hex(), "userId", entry.UserID.Hex(),
			"stored", stored, "ranking", entry.Value)
		return
	}

	// Refetch the user to capture fresh equipment / skills before recording.
	userRaw, err := client.GetUserForDamage(ctx, entry.UserID)
	if err != nil {
		slog.Error("Failed fetching user for damage",
			"userId", entry.UserID.Hex(), "error", err)
		return
	}
	user := in.User(ctx, userRaw)
	if user == nil {
		return
	}

	skill, err := in.colls.Trackers.Skill.GetLatestForUser(ctx, entry.UserID)
	if err != nil {
		slog.Error("Failed reading latest skill snapshot",
			"userId", entry.UserID.Hex(), "error", err)
		return
	}
	if skill == nil {
		slog.Warn("No skill snapshot for damage attribution; skipping",
			"userId", entry.UserID.Hex(), "battleId", battleID.Hex())
		return
	}

	currentEquip := parseEquipment(user.LatestObject)
	resolved := in.resolveLoadout(ctx, entry.UserID, currentEquip, prevPollAt, client)

	dmg := trackers.Damage{
		BattleID:     battleID,
		Side:         side,
		UserID:       entry.UserID,
		CountryID:    user.CountryID,
		MuID:         user.MuID,
		PartyID:      user.PartyID,
		WeaponID:     resolved.Weapon,
		Ammo:         resolved.Ammo,
		HelmetID:     resolved.Helmet,
		ChestID:      resolved.Chest,
		PantsID:      resolved.Pants,
		BootsID:      resolved.Boots,
		GlovesID:     resolved.Gloves,
		SkillID:      skill.ID,
		MilitaryRank: user.MilitaryRank,
		Damages:      delta,
		At:           time.Now().UTC(),
	}
	in.batchers.Damage.Add(dmg)

	in.lastSeen.Mark(entry.UserID)
}

// BattleFinalize pulls the final battle object via /battle.getById and
// finalises the tracker (winner, end time, final per-side damages, raw).
// One last ranking pass is executed for both sides so the cumulative
// totals catch up to whatever the upstream reported at end-of-battle.
func (in *Ingester) BattleFinalize(
	ctx context.Context,
	battleID bson.ObjectID,
	prevPollAt time.Time,
	client *gateway.Client,
) {
	// Last ranking sweep to absorb any final damage events.
	g, gctx := errgroup.WithContext(ctx)
	for _, side := range []enums.Side{enums.ATTACKER, enums.DEFENDER} {
		side := side
		g.Go(func() error {
			raw, err := client.GetBattleRanking(gctx, battleID, side)
			if err != nil {
				slog.Error("Failed final battle ranking",
					"battleId", battleID.Hex(), "side", side, "error", err)
				return nil
			}
			in.BattleRanking(gctx, battleID, side, raw, prevPollAt, client)
			return nil
		})
	}
	_ = g.Wait()

	raw, err := client.GetBattleByID(ctx, battleID)
	if err != nil {
		slog.Error("Failed fetching final battle object",
			"battleId", battleID.Hex(), "error", err)
		return
	}

	var payload struct {
		IsActive bool      `json:"isActive"`
		WonBy    *string   `json:"wonBy,omitempty"`
		EndedAt  time.Time `json:"endedAt"`
		Attacker struct {
			Damages int `json:"damages"`
		} `json:"attacker"`
		Defender struct {
			Damages int `json:"damages"`
		} `json:"defender"`
	}
	err = json.Unmarshal(raw, &payload)
	if err != nil {
		slog.Error("Failed unmarshalling final battle object",
			"battleId", battleID.Hex(), "error", err)
		return
	}
	if payload.WonBy == nil {
		slog.Warn("Battle finalize called but wonBy not set yet",
			"battleId", battleID.Hex(), "isActive", payload.IsActive)
		return
	}

	endedAt := payload.EndedAt
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}

	err = in.colls.Trackers.Battle.SetWinner(
		ctx, battleID, *payload.WonBy, endedAt,
		payload.Attacker.Damages, payload.Defender.Damages, raw,
	)
	if err != nil {
		slog.Error("Failed finalising battle",
			"battleId", battleID.Hex(), "error", err)
	}
}

// parseEquipment extracts the equipment block from a raw user payload.
// Returns nil if the payload has no equipment or fails to parse.
func parseEquipment(raw json.RawMessage) *userEquipmentPayload {
	if len(raw) == 0 {
		return nil
	}
	var u userRawForDamage
	err := json.Unmarshal(raw, &u)
	if err != nil {
		slog.Error("Failed unmarshalling user equipment block", "error", err)
		return nil
	}
	return u.Equipment
}

// resolveLoadout chooses an item ID per equipment slot. Per slot:
//
//  1. If a DismantleTransaction by `userID` in [prevPollAt, now] mentions an
//     item that's not the currently-equipped one for that slot, prefer the
//     dismantled item — the user clearly used it before swapping.
//  2. Otherwise, use the currently-equipped item ID.
//
// If the currently-equipped item is unknown to our DB, a single
// `inventory.fetchCurrentEquipment` call hydrates all six slots via
// `Item.UpsertEquipment`. The hydrated IDs always remain the slot value;
// only dismantle-attribution can override.
func (in *Ingester) resolveLoadout(
	ctx context.Context,
	userID bson.ObjectID,
	current *userEquipmentPayload,
	prevPollAt time.Time,
	client *gateway.Client,
) loadout {
	var out loadout
	if current == nil {
		return out
	}

	out.Weapon = current.Weapon
	out.Helmet = current.Helmet
	out.Chest = current.Chest
	out.Pants = current.Pants
	out.Boots = current.Boots
	out.Gloves = current.Gloves
	out.Ammo = current.Ammo

	in.hydrateUnknownEquipment(ctx, userID, current, client)

	dismantles, err := in.colls.Transactions.DismantleTransaction.GetByUserSince(ctx, userID, prevPollAt)
	if err != nil {
		slog.Error("Failed loading dismantle history",
			"userId", userID.Hex(), "since", prevPollAt, "error", err)
		return out
	}
	if len(dismantles) == 0 {
		return out
	}

	// Newest-first: the first dismantle for each slot wins (most recent).
	seen := make(map[equipmentSlot]struct{}, 6)
	for _, d := range dismantles {
		slot, err := in.classifyDismantledItem(ctx, d.ItemID)
		if err != nil {
			slog.Error("Failed classifying dismantled item",
				"itemId", d.ItemID.Hex(), "error", err)
			continue
		}
		if slot == slotUnknown {
			continue
		}
		if _, dup := seen[slot]; dup {
			continue
		}
		seen[slot] = struct{}{}

		// Only override if the dismantled item differs from the currently-
		// equipped one. If they match, the current value is already correct.
		curr := slotPointer(&out, slot)
		dID := d.ItemID
		if curr != nil && *curr == dID {
			continue
		}
		assignSlot(&out, slot, &dID)
	}

	return out
}

func (in *Ingester) hydrateUnknownEquipment(
	ctx context.Context,
	userID bson.ObjectID,
	current *userEquipmentPayload,
	client *gateway.Client,
) {
	ids := []*bson.ObjectID{
		current.Weapon, current.Helmet, current.Chest,
		current.Pants, current.Boots, current.Gloves,
	}
	needsHydration := false
	for _, p := range ids {
		if p == nil || p.IsZero() {
			continue
		}
		// A placeholder has empty itemCode; we treat that as "unknown" too so
		// the damage record references a fully-populated item where possible.
		doc, err := in.colls.Trackers.Item.Get(ctx, *p)
		if errors.Is(err, mongo.ErrNoDocuments) {
			needsHydration = true
			break
		}
		if err != nil {
			slog.Error("Failed checking equipment item",
				"userId", userID.Hex(), "itemId", p.Hex(), "error", err)
			continue
		}
		if doc.ItemCode == "" {
			needsHydration = true
			break
		}
	}
	if !needsHydration {
		return
	}

	raw, err := client.GetUserEquipment(ctx, userID)
	if err != nil {
		slog.Error("Failed fetching equipment for damage attribution",
			"userId", userID.Hex(), "error", err)
		return
	}

	var eq equipmentResponse
	err = json.Unmarshal(raw, &eq)
	if err != nil {
		slog.Error("Failed unmarshalling equipment payload",
			"userId", userID.Hex(), "error", err)
		return
	}

	for _, item := range []*equipmentItem{eq.Weapon, eq.Helmet, eq.Chest, eq.Pants, eq.Boots, eq.Gloves} {
		if item == nil || item.ID.IsZero() {
			continue
		}

		err := in.colls.Trackers.Item.UpsertEquipment(
			ctx, item.ID, item.Code, item.Skills, item.State, userID,
		)
		if err != nil {
			slog.Error("Failed upserting equipment item",
				"userId", userID.Hex(), "itemId", item.ID.Hex(), "error", err)
		}
	}
}

// classifyDismantledItem returns which slot a dismantled item belonged to,
// inferred from its itemCode prefix. Unknown / placeholder items return
// slotUnknown so they're skipped silently.
func (in *Ingester) classifyDismantledItem(ctx context.Context, itemID bson.ObjectID) (equipmentSlot, error) {
	doc, err := in.colls.Trackers.Item.Get(ctx, itemID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return slotUnknown, nil
		}
		return slotUnknown, err
	}
	return slotFromItemCode(doc.ItemCode), nil
}

// slotFromItemCode classifies an item code into its equipment slot.
func slotFromItemCode(code string) equipmentSlot {
	switch code {
	case "":
		return slotUnknown
	case "knife", "gun", "rifle", "sniper", "tank", "jet":
		return slotWeapon
	}
	switch {
	case strings.HasPrefix(code, "helmet"):
		return slotHelmet
	case strings.HasPrefix(code, "chest"):
		return slotChest
	case strings.HasPrefix(code, "pants"):
		return slotPants
	case strings.HasPrefix(code, "boots"):
		return slotBoots
	case strings.HasPrefix(code, "gloves"):
		return slotGloves
	}
	return slotUnknown
}

func slotPointer(l *loadout, slot equipmentSlot) *bson.ObjectID {
	switch slot {
	case slotWeapon:
		return l.Weapon
	case slotHelmet:
		return l.Helmet
	case slotChest:
		return l.Chest
	case slotPants:
		return l.Pants
	case slotBoots:
		return l.Boots
	case slotGloves:
		return l.Gloves
	}
	return nil
}

func assignSlot(l *loadout, slot equipmentSlot, id *bson.ObjectID) {
	switch slot {
	case slotWeapon:
		l.Weapon = id
	case slotHelmet:
		l.Helmet = id
	case slotChest:
		l.Chest = id
	case slotPants:
		l.Pants = id
	case slotBoots:
		l.Boots = id
	case slotGloves:
		l.Gloves = id
	}
}
