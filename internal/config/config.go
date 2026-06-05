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
}

func Load() (Config, error) {
	cfg := Config{
		GatewayAddr:              os.Getenv("GATEWAY_ADDR"),
		HTTPTimeout:              getDuration("HTTP_TIMEOUT", 30*time.Second),
		TransactionsInterval:     getDuration("TRANSACTIONS_INTERVAL", 5*time.Second),
		UsersInterval:            getDuration("USERS_INTERVAL", 5*time.Second),
		UserQueueInterval:        getDuration("USER_QUEUE_INTERVAL", time.Second),
		UserQueueBuffer:          getInt("USER_QUEUE_BUFFER", 1000),
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
