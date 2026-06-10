package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/warerastats/models/models/stores/events"
	"github.com/warerastats/models/models/stores/trackers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type countryTaxes struct {
	Income   float64 `json:"income"`
	Market   float64 `json:"market"`
	SelfWork float64 `json:"selfWork"`
}

type countryPayload struct {
	ID              bson.ObjectID  `json:"_id"`
	Name            string         `json:"name"`
	Code            string         `json:"code"`
	Money           float64        `json:"money"`
	Taxes           countryTaxes   `json:"taxes"`
	SpecializedItem *string        `json:"specializedItem,omitempty"`
	RulingParty     *bson.ObjectID `json:"rulingParty,omitempty"`
	AllianceID      *bson.ObjectID `json:"allianceId,omitempty"`
}

// Countries parses the full /country.getAllCountries payload, upserts each
// country tracker, and emits ruling-party / specialisation change events
// when those fields differ from what is already stored.
func (in *Ingester) Countries(ctx context.Context, raw json.RawMessage) {
	var list []json.RawMessage
	err := json.Unmarshal(raw, &list)
	if err != nil {
		slog.Error("Failed unmarshalling countries list", "error", err)
		return
	}

	for _, countryRaw := range list {
		var p countryPayload
		err := json.Unmarshal(countryRaw, &p)
		if err != nil {
			slog.Error("Failed unmarshalling country", "error", err)
			continue
		}
		if p.ID.IsZero() {
			continue
		}

		in.emitCountryEvents(ctx, p)

		country := trackers.Country{
			Name:          p.Name,
			Code:          p.Code,
			Money:         p.Money,
			RulingPartyID: p.RulingParty,
			AllianceID:    p.AllianceID,
			LatestObject:  countryRaw,
		}
		country.Taxes.Income = p.Taxes.Income
		country.Taxes.Market = p.Taxes.Market
		country.Taxes.SelfWork = p.Taxes.SelfWork
		country.SpecialisationItemCode = p.SpecializedItem

		err = in.colls.Trackers.Country.UpsertCountry(ctx, p.ID, country)
		if err != nil {
			slog.Error("Failed upserting country", "countryId", p.ID.Hex(), "error", err)
		}
	}
}

func (in *Ingester) emitCountryEvents(ctx context.Context, p countryPayload) {
	logEvent := func(name string, err error) {
		if err != nil {
			slog.Error("Failed writing country change event", "event", name, "countryId", p.ID.Hex(), "error", err)
		}
	}

	prev, err := in.colls.Trackers.Country.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior country", "countryId", p.ID.Hex(), "error", err)
	}

	// Specialisation
	var prevSpec *string
	if prev != nil && prev.SpecialisationItemCode != nil {
		s := prev.SpecialisationItemCode
		prevSpec = s
	}
	if prev == nil || !stringPtrEqual(prevSpec, p.SpecializedItem) {
		logEvent("specialisation", in.colls.Events.CountrySpecialisationChange.Set(ctx, events.CountrySpecialisationChange{
			CountryID:              p.ID,
			SpecialisationItemCode: p.SpecializedItem,
		}))
	}

	// Ruling party
	var prevRuler *bson.ObjectID
	if prev != nil {
		prevRuler = prev.RulingPartyID
	}
	if prev == nil || !objectIDPtrEqual(prevRuler, p.RulingParty) {
		logEvent("rulingParty", in.colls.Events.CountryRulingPartyChange.Set(ctx, events.CountryRulingPartyChange{
			CountryID: p.ID,
			PartyID:   p.RulingParty,
		}))
	}

	// Alliance
	var prevAlliance *bson.ObjectID
	if prev != nil {
		prevAlliance = prev.AllianceID
	}
	if prev == nil || !objectIDPtrEqual(prevAlliance, p.AllianceID) {
		if p.AllianceID != nil {
			logEvent("allianceJoin", in.colls.Events.CountryAllianceJoin.Set(ctx, events.CountryAllianceJoin{
				CountryID:  p.ID,
				AllianceID: *p.AllianceID,
			}))
			// Ensure alliance tracker exists.
			missing, err := in.colls.Trackers.Alliance.Exists(ctx, []bson.ObjectID{*p.AllianceID})
			if err != nil {
				slog.Error("Failed checking alliance existence", "allianceId", p.AllianceID.Hex(), "error", err)
			} else if len(missing) > 0 {
				if err := in.colls.Trackers.Alliance.CreateEmpty(ctx, *p.AllianceID); err != nil {
					slog.Error("Failed creating empty alliance", "allianceId", p.AllianceID.Hex(), "error", err)
				}
			}
		} else if prevAlliance != nil {
			logEvent("allianceLeave", in.colls.Events.CountryAllianceLeave.Set(ctx, events.CountryAllianceLeave{
				CountryID:      p.ID,
				PrevAllianceID: prevAlliance,
			}))
		}
	}
}
