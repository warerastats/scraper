package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/config"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"github.com/warerastats/scraper/internal/scheduler"
	"github.com/warerastats/scraper/internal/userqueue"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("Scraper starting")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed loading config", "error", err)
		os.Exit(1)
	}

	colls, err := models.Init(ctx)
	if err != nil {
		slog.Error("Failed connecting to the database!", "error", err)
		os.Exit(1)
	}
	defer colls.Close(ctx)

	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}
	client := gateway.NewClient(cfg.GatewayAddr, httpClient)

	queue := userqueue.New(colls, cfg.UserQueueBuffer, cfg.UserQueueInterval)
	ingester := ingest.New(colls, queue)

	txScheduler := &scheduler.Transactions{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.TransactionsInterval,
		Workers:  cfg.WorkerPoolSize,
	}
	usersScheduler := &scheduler.Users{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.UsersInterval,
		Workers:  cfg.WorkerPoolSize,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return queue.Run(gctx) })
	g.Go(func() error { return txScheduler.Run(gctx) })
	g.Go(func() error { return usersScheduler.Run(gctx) })

	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Scraper exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("Scraper stopped")
}
