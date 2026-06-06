package ingest

import (
	"context"
	"log/slog"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/lastseen"
	"github.com/warerastats/scraper/internal/muqueue"
	"github.com/warerastats/scraper/internal/partyqueue"
	"github.com/warerastats/scraper/internal/userqueue"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Ingester turns raw JSON payloads from the gateway into database writes.
// It also feeds the user-exists queue when transactions reference users
// that may not be tracked yet, and marks lastSeen so the refresh scheduler
// can prioritise active users. The mu and party queues / flushers play the
// same role for their respective entities.
type Ingester struct {
	colls         *models.Collections
	queue         *userqueue.Queue
	lastSeen      *lastseen.Flusher
	muQueue       *muqueue.Queue
	partyQueue    *partyqueue.Queue
	muLastSeen    *lastseen.MuFlusher
	partyLastSeen *lastseen.PartyFlusher
	batchers      *Batchers
}

func New(
	colls *models.Collections,
	queue *userqueue.Queue,
	lastSeen *lastseen.Flusher,
	muQueue *muqueue.Queue,
	partyQueue *partyqueue.Queue,
	muLastSeen *lastseen.MuFlusher,
	partyLastSeen *lastseen.PartyFlusher,
	batchers *Batchers,
) *Ingester {
	return &Ingester{
		colls:         colls,
		queue:         queue,
		lastSeen:      lastSeen,
		muQueue:       muQueue,
		partyQueue:    partyQueue,
		muLastSeen:    muLastSeen,
		partyLastSeen: partyLastSeen,
		batchers:      batchers,
	}
}

// FlushTransactions drains the buffered transaction writers. Called by the
// transactions scheduler before it advances its checkpoint.
func (in *Ingester) FlushTransactions(ctx context.Context) error {
	return in.batchers.FlushTransactions(ctx)
}

// enqueueMissingUsers deduplicates ids, drops zero values, and enqueues only
// those user IDs not already tracked onto the user-exists queue. Membership
// rosters (mu / party) re-list the same users on every refresh, so blindly
// enqueuing them all would flood the queue with IDs that already exist. The
// single batched Exists check at the source keeps the queue near-empty in
// steady state. On a lookup error it falls back to enqueuing everything so
// members are still eventually discovered.
func (in *Ingester) enqueueMissingUsers(ctx context.Context, ids []bson.ObjectID) {
	seen := make(map[bson.ObjectID]struct{}, len(ids))
	uniq := make([]bson.ObjectID, 0, len(ids))
	for _, id := range ids {
		if id.IsZero() {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	if len(uniq) == 0 {
		return
	}

	missing, err := in.colls.Trackers.User.Exists(ctx, uniq)
	if err != nil {
		slog.Error("Failed checking member existence; enqueuing all", "count", len(uniq), "error", err)
		missing = uniq
	}
	for _, id := range missing {
		in.queue.Enqueue(id)
	}
}

// Item is the embedded item payload shared by several transaction types.
type Item struct {
	ID     bson.ObjectID      `json:"_id"`
	Code   string             `json:"code"`
	Skills map[string]float64 `json:"skills"`
	State  int                `json:"state"`
}

// CaseRarity counts case openings by rarity tier.
type CaseRarity struct {
	Uncommon  *int `json:"uncommon,omitempty"`
	Common    *int `json:"common,omitempty"`
	Rare      *int `json:"rare,omitempty"`
	Epic      *int `json:"epic,omitempty"`
	Legendary *int `json:"legendary,omitempty"`
	Mythic    *int `json:"mythic,omitempty"`
}

// CaseStats wraps a CaseRarity behind the `byRarity` JSON field used by the API.
type CaseStats struct {
	ByRarity CaseRarity `json:"byRarity"`
}
