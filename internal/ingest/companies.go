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

type companyPayload struct {
	ID       bson.ObjectID `json:"_id"`
	User     bson.ObjectID `json:"user"`
	Region   bson.ObjectID `json:"region"`
	ItemCode string        `json:"itemCode"`
	Name     string        `json:"name"`
}

type workersPayload struct {
	Type    string          `json:"type"`
	Workers []workerPayload `json:"workers"`
}

type workerPayload struct {
	ID                     bson.ObjectID `json:"_id"`
	User                   bson.ObjectID `json:"user"`
	Company                bson.ObjectID `json:"company"`
	Employer               bson.ObjectID `json:"employer"`
	Wage                   float64       `json:"wage"`
	JoinedAt               time.Time     `json:"joinedAt"`
	Fidelity               int           `json:"fidelity"`
	LastFidelityIncreaseAt time.Time     `json:"lastFidelityIncreaseAt"`
	Raw                    json.RawMessage
}

func (w *workerPayload) UnmarshalJSON(data []byte) error {
	type Alias workerPayload
	var a Alias
	err := json.Unmarshal(data, &a)
	if err != nil {
		return err
	}
	*w = workerPayload(a)
	w.Raw = data
	return nil
}

// Company parses a raw company document from the gateway, emits region /
// itemCode change events (seed on first track, diff on change), and upserts
// the company tracker. Returns the parsed company ID and a success flag.
func (in *Ingester) Company(ctx context.Context, raw json.RawMessage) (bson.ObjectID, bool) {
	var p companyPayload
	err := json.Unmarshal(raw, &p)
	if err != nil {
		slog.Error("Failed unmarshalling company", "error", err)
		return bson.ObjectID{}, false
	}
	if p.ID.IsZero() {
		return bson.ObjectID{}, false
	}

	prev, err := in.colls.Trackers.Company.Get(ctx, p.ID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior company", "companyId", p.ID.Hex(), "error", err)
	}

	in.emitCompanyEvents(ctx, p, prev)

	err = in.colls.Trackers.Company.Upsert(ctx, p.ID, trackers.Company{
		UserID:       p.User,
		RegionID:     p.Region,
		ItemCode:     p.ItemCode,
		Name:         p.Name,
		LatestObject: raw,
	})
	if err != nil {
		slog.Error("Failed upserting company", "companyId", p.ID.Hex(), "error", err)
		return p.ID, false
	}

	return p.ID, true
}

func (in *Ingester) emitCompanyEvents(ctx context.Context, p companyPayload, prev *trackers.Company) {
	logEvent := func(name string, err error) {
		if err != nil {
			slog.Error("Failed writing company change event", "event", name, "companyId", p.ID.Hex(), "error", err)
		}
	}

	if prev == nil || prev.RegionID != p.Region {
		logEvent("region", in.colls.Events.CompanyRegionChange.Set(ctx, events.CompanyRegionChange{
			CompanyID: p.ID,
			RegionID:  p.Region,
		}))
	}
	if prev == nil || prev.ItemCode != p.ItemCode {
		logEvent("itemCode", in.colls.Events.CompanyItemCodeChange.Set(ctx, events.CompanyItemCodeChange{
			CompanyID: p.ID,
			ItemCode:  p.ItemCode,
		}))
	}
}

// Workers parses the worker.getWorkers response for a company. For every
// worker it emits an EmployeeWageChange (seed-on-first / diff-on-change) and
// a UserCompanyChange (only when the latest known assignment differs), then
// upserts the Employee tracker. Returns the set of worker user IDs so the
// caller can compute which prior employees have left.
func (in *Ingester) Workers(ctx context.Context, raw json.RawMessage, companyID bson.ObjectID) []bson.ObjectID {
	var p workersPayload
	err := json.Unmarshal(raw, &p)
	if err != nil {
		slog.Error("Failed unmarshalling workers", "companyId", companyID.Hex(), "error", err)
		return nil
	}

	userIDs := make([]bson.ObjectID, 0, len(p.Workers))
	for _, w := range p.Workers {
		if w.ID.IsZero() || w.User.IsZero() {
			continue
		}

		in.emitWorkerEvents(ctx, w, companyID)

		err := in.colls.Trackers.Employee.Upsert(ctx, w.ID, trackers.Employee{
			UserID:                 w.User,
			CompanyID:              w.Company,
			EmployerID:             w.Employer,
			Wage:                   w.Wage,
			Fidelity:               w.Fidelity,
			JoinedAt:               w.JoinedAt,
			LastFidelityIncreaseAt: w.LastFidelityIncreaseAt,
			LatestObject:           w.Raw,
		})
		if err != nil {
			slog.Error("Failed upserting employee", "employeeId", w.ID.Hex(), "userId", w.User.Hex(), "error", err)
			continue
		}

		userIDs = append(userIDs, w.User)
		in.queue.Enqueue(w.User)
		in.lastSeen.Mark(w.User)
	}

	return userIDs
}

func (in *Ingester) emitWorkerEvents(ctx context.Context, w workerPayload, companyID bson.ObjectID) {
	// Wage
	prevWage, err := in.colls.Events.EmployeeWageChange.Get(ctx, w.User)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior employee wage", "userId", w.User.Hex(), "error", err)
	}
	if prevWage == nil || prevWage.CompanyID != companyID || prevWage.Wage != w.Wage {
		err := in.colls.Events.EmployeeWageChange.Set(ctx, events.EmployeeWageChange{
			UserID:    w.User,
			CompanyID: companyID,
			Wage:      w.Wage,
		})
		if err != nil {
			slog.Error("Failed writing employee wage change", "userId", w.User.Hex(), "error", err)
		}
	}

	// Company assignment — mirror into UserCompanyChange when the latest
	// known assignment is missing or differs from the current company.
	prevCompany, err := in.colls.Events.UserCompanyChange.Get(ctx, w.User)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior user company", "userId", w.User.Hex(), "error", err)
	}
	cID := companyID
	if prevCompany == nil || !objectIDPtrEqual(prevCompany.CompanyID, &cID) {
		err := in.colls.Events.UserCompanyChange.Set(ctx, events.UserCompanyChange{
			UserID:    w.User,
			CompanyID: &cID,
		})
		if err != nil {
			slog.Error("Failed writing user company change", "userId", w.User.Hex(), "error", err)
		}
	}
}

// MarkWorkerLeft is invoked for prior employees of a company that no longer
// appear in the latest worker.getWorkers response. It appends a nil-company
// UserCompanyChange (only if the latest known assignment was non-nil) and
// deletes the Employee tracker row.
func (in *Ingester) MarkWorkerLeft(ctx context.Context, employee trackers.Employee) {
	prevCompany, err := in.colls.Events.UserCompanyChange.Get(ctx, employee.UserID)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		slog.Error("Failed loading prior user company", "userId", employee.UserID.Hex(), "error", err)
	}
	if prevCompany == nil || prevCompany.CompanyID != nil {
		err := in.colls.Events.UserCompanyChange.Set(ctx, events.UserCompanyChange{
			UserID:    employee.UserID,
			CompanyID: nil,
		})
		if err != nil {
			slog.Error("Failed writing user company change (left)", "userId", employee.UserID.Hex(), "error", err)
		}
	}

	err = in.colls.Trackers.Employee.Delete(ctx, employee.ID)
	if err != nil {
		slog.Error("Failed deleting employee", "employeeId", employee.ID.Hex(), "error", err)
	}
}
