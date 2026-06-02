package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GatewayAddr          string
	HTTPTimeout          time.Duration
	TransactionsInterval time.Duration
	UsersInterval        time.Duration
	UserQueueInterval    time.Duration
	UserQueueBuffer      int
	WorkerPoolSize       int
}

func Load() (Config, error) {
	cfg := Config{
		GatewayAddr:          os.Getenv("GATEWAY_ADDR"),
		HTTPTimeout:          getDuration("HTTP_TIMEOUT", 30*time.Second),
		TransactionsInterval: getDuration("TRANSACTIONS_INTERVAL", 5*time.Second),
		UsersInterval:        getDuration("USERS_INTERVAL", 5*time.Second),
		UserQueueInterval:    getDuration("USER_QUEUE_INTERVAL", time.Second),
		UserQueueBuffer:      getInt("USER_QUEUE_BUFFER", 1000),
		WorkerPoolSize:       getInt("WORKER_POOL_SIZE", 32),
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
