package ingest

import (
	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/userqueue"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Ingester turns raw JSON payloads from the gateway into database writes.
// It also feeds the user-exists queue when transactions reference users
// that may not be tracked yet.
type Ingester struct {
	colls *models.Collections
	queue *userqueue.Queue
}

func New(colls *models.Collections, queue *userqueue.Queue) *Ingester {
	return &Ingester{colls: colls, queue: queue}
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
