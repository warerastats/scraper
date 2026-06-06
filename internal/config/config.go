package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GatewayAddr              string
	HTTPTimeout              time.Duration
	TransactionsInterval     time.Duration
	UsersInterval            time.Duration
	UserQueueInterval        time.Duration
	UserQueueBuffer          int
	WorkerPoolSize           int
	RefreshInterval          time.Duration
	RefreshTarget            int
	RegionsInterval          time.Duration
	CountriesInterval        time.Duration
	CompaniesInterval        time.Duration
	CompaniesBackfillMax     time.Duration
	LastSeenInterval         time.Duration
	LastSeenRecentThreshold  time.Duration
	BattleRankingSweepPeriod time.Duration
	TradeOffersInterval      time.Duration
	TradeOffersLimit         int

	MuInterval         time.Duration
	MuRefreshInterval  time.Duration
	MuRefreshTarget    int
	MuQueueInterval    time.Duration
	MuQueueBuffer      int
	MuLastSeenInterval time.Duration

	PartyInterval         time.Duration
	PartyRefreshInterval  time.Duration
	PartyRefreshTarget    int
	PartyQueueInterval    time.Duration
	PartyQueueBuffer      int
	PartyLastSeenInterval time.Duration
	RulingPartyMaxAge     time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		GatewayAddr:              os.Getenv("GATEWAY_ADDR"),
		HTTPTimeout:              getDuration("HTTP_TIMEOUT", 30*time.Second),
		TransactionsInterval:     getDuration("TRANSACTIONS_INTERVAL", 5*time.Second),
		UsersInterval:            getDuration("USERS_INTERVAL", 5*time.Second),
		UserQueueInterval:        getDuration("USER_QUEUE_INTERVAL", time.Second),
		UserQueueBuffer:          getInt("USER_QUEUE_BUFFER", 5000),
		WorkerPoolSize:           getInt("WORKER_POOL_SIZE", 32),
		RefreshInterval:          getDuration("REFRESH_INTERVAL", 3*time.Second),
		RefreshTarget:            getInt("REFRESH_TARGET", 100),
		RegionsInterval:          getDuration("REGIONS_INTERVAL", 5*time.Second),
		CountriesInterval:        getDuration("COUNTRIES_INTERVAL", 5*time.Minute),
		CompaniesInterval:        getDuration("COMPANIES_INTERVAL", 10*time.Minute),
		CompaniesBackfillMax:     getDuration("COMPANIES_BACKFILL_MAX", 60*time.Minute),
		LastSeenInterval:         getDuration("LAST_SEEN_INTERVAL", 5*time.Second),
		LastSeenRecentThreshold:  getDuration("LAST_SEEN_RECENT_THRESHOLD", 24*time.Hour),
		BattleRankingSweepPeriod: getDuration("BATTLE_RANKING_SWEEP_PERIOD", 120*time.Second),
		TradeOffersInterval:      getDuration("TRADE_OFFERS_INTERVAL", 5*time.Second),
		TradeOffersLimit:         getInt("TRADE_OFFERS_LIMIT", 100),

		MuInterval:         getDuration("MU_INTERVAL", 5*time.Second),
		MuRefreshInterval:  getDuration("MU_REFRESH_INTERVAL", 3*time.Second),
		MuRefreshTarget:    getInt("MU_REFRESH_TARGET", 50),
		MuQueueInterval:    getDuration("MU_QUEUE_INTERVAL", time.Second),
		MuQueueBuffer:      getInt("MU_QUEUE_BUFFER", 5000),
		MuLastSeenInterval: getDuration("MU_LAST_SEEN_INTERVAL", 5*time.Second),

		PartyInterval:         getDuration("PARTY_INTERVAL", 5*time.Second),
		PartyRefreshInterval:  getDuration("PARTY_REFRESH_INTERVAL", 3*time.Second),
		PartyRefreshTarget:    getInt("PARTY_REFRESH_TARGET", 50),
		PartyQueueInterval:    getDuration("PARTY_QUEUE_INTERVAL", time.Second),
		PartyQueueBuffer:      getInt("PARTY_QUEUE_BUFFER", 5000),
		PartyLastSeenInterval: getDuration("PARTY_LAST_SEEN_INTERVAL", 5*time.Second),
		RulingPartyMaxAge:     getDuration("RULING_PARTY_MAX_AGE", time.Hour),
	}

	if cfg.GatewayAddr == "" {
		return cfg, errors.New("GATEWAY_ADDR is required")
	}
	return cfg, nil
}

func getDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func getInt(key string, def int) int {
	v := os.Getenv(key)
	if v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			return i
		}
	}
	return def
}
