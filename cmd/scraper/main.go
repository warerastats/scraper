package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/handlers"
	"github.com/warerastats/scraper/internal/timer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	slog.Info("Scraper starting")

	colls, err := models.Init(ctx)
	if err != nil {
		slog.Error("Failed connecting to the database!", "error", err)
		os.Exit(1)
	}
	defer colls.Close(ctx)

	go handlers.HandleUsersExistsLoop(ctx, colls)
	timer.Transactions(ctx, colls)
}
