package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"
)

// CompanyOwnership periodically checks active users for owned companies via
// the upstream API and fetches any that are not yet tracked. This covers
// users who own companies but have no employees (and therefore never appear
// in wage transactions).
type CompanyOwnership struct {
	Client   *gateway.Client
	Ingester *ingest.Ingester
	Colls    *models.Collections
	Interval time.Duration
	Offset   time.Duration
	Target   int
	Workers  int

	mu       sync.Mutex
	inFlight map[bson.ObjectID]struct{}
}

func (s *CompanyOwnership) Run(ctx context.Context) error {
	s.inFlight = make(map[bson.ObjectID]struct{}, s.Target)

	if !waitOffset(ctx, s.Offset) {
		return ctx.Err()
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		s.tick(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *CompanyOwnership) tick(ctx context.Context) {
	s.mu.Lock()
	pending := len(s.inFlight)
	need := s.Target - pending
	exclude := make([]bson.ObjectID, 0, pending)
	for id := range s.inFlight {
		exclude = append(exclude, id)
	}
	s.mu.Unlock()

	if need <= 0 {
		return
	}

	ids, err := s.Colls.Trackers.User.GetForCompanyOwnershipCheck(ctx, need, exclude)
	if err != nil {
		slog.Error("Failed getting users for company ownership check", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	s.mu.Lock()
	for _, id := range ids {
		s.inFlight[id] = struct{}{}
	}
	s.mu.Unlock()

	for _, id := range ids {
		go s.checkOne(ctx, id)
	}

	slog.Info("CompanyOwnership tick: dispatched", "added", len(ids), "inFlight", pending+len(ids), "target", s.Target)
}

func (s *CompanyOwnership) checkOne(ctx context.Context, userID bson.ObjectID) {
	defer func() {
		s.mu.Lock()
		delete(s.inFlight, userID)
		s.mu.Unlock()
	}()

	raw, err := s.Client.GetUserCompanies(ctx, userID)
	if err != nil {
		slog.Error("Failed fetching user companies", "userId", userID.Hex(), "error", err)
		return
	}

	var resp struct {
		Items []string `json:"items"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		slog.Error("Failed unmarshalling user companies", "userId", userID.Hex(), "error", err)
		return
	}

	companyIDs := make([]bson.ObjectID, 0, len(resp.Items))
	for _, hex := range resp.Items {
		id, err := bson.ObjectIDFromHex(hex)
		if err != nil {
			slog.Error("Invalid company ID from user companies", "userId", userID.Hex(), "companyId", hex, "error", err)
			continue
		}
		companyIDs = append(companyIDs, id)
	}

	if len(companyIDs) > 0 {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(s.Workers)
		for _, cid := range companyIDs {
			cid := cid
			g.Go(func() error {
				s.processCompany(gctx, cid)
				return nil
			})
		}
		_ = g.Wait()
	}

	if err := s.Colls.Trackers.User.SetLastCompanyCheck(ctx, userID, time.Now().UTC()); err != nil {
		slog.Error("Failed setting lastCompanyCheck", "userId", userID.Hex(), "error", err)
	}
}

func (s *CompanyOwnership) processCompany(ctx context.Context, companyID bson.ObjectID) {
	prevEmployees, err := s.Colls.Trackers.Employee.GetByCompany(ctx, companyID)
	if err != nil {
		slog.Error("Failed loading prior employees", "companyId", companyID.Hex(), "error", err)
	}

	var companyRaw, workersRaw []byte
	cg, cgctx := errgroup.WithContext(ctx)
	cg.Go(func() error {
		raw, err := s.Client.GetCompany(cgctx, companyID)
		if err != nil {
			return err
		}
		companyRaw = raw
		return nil
	})
	cg.Go(func() error {
		raw, err := s.Client.GetWorkers(cgctx, companyID)
		if err != nil {
			return err
		}
		workersRaw = raw
		return nil
	})

	err = cg.Wait()
	if err != nil {
		slog.Error("Failed fetching company data", "companyId", companyID.Hex(), "error", err)
		return
	}

	s.Ingester.Company(ctx, companyRaw)
	currentUserIDs := s.Ingester.Workers(ctx, workersRaw, companyID)

	current := make(map[bson.ObjectID]struct{}, len(currentUserIDs))
	for _, id := range currentUserIDs {
		current[id] = struct{}{}
	}
	for _, prev := range prevEmployees {
		if _, ok := current[prev.UserID]; ok {
			continue
		}
		s.Ingester.MarkWorkerLeft(ctx, prev)
	}
}
