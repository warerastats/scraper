package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/warerastats/models/models"
)

func main() {
	ctx := context.Background()
	slog.Info("Scraper starting")

	colls, err := models.Init(ctx)
	if err != nil {
		slog.Error("Failed connecting to the database!", "error", err)
		os.Exit(1)
	}
	defer colls.Close(ctx)

	for {
		time.Sleep(5 * time.Second)
		slog.Info("hi")
	}
}
