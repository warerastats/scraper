package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/scraper/internal/config"
	"github.com/warerastats/scraper/internal/gateway"
	"github.com/warerastats/scraper/internal/ingest"
	"github.com/warerastats/scraper/internal/lastseen"
	"github.com/warerastats/scraper/internal/muqueue"
	"github.com/warerastats/scraper/internal/partyqueue"
	"github.com/warerastats/scraper/internal/scheduler"
	"github.com/warerastats/scraper/internal/userqueue"
	"golang.org/x/sync/errgroup"
)

// Scheduler start-up phase offsets stagger each periodic scheduler's first
// tick so their upstream requests spread across the polling window instead of
// bursting together at start-up and on every shared interval boundary. The 5s
// schedulers are spread across a 5s window and the 3s refresh schedulers across
// a 3s window; same-interval schedulers keep their phase, so the spacing holds
// for every subsequent tick. BattleRanking is adaptive and self-spreading.
const (
	offsetTransactions = 0 * time.Millisecond
	offsetRefresh      = 500 * time.Millisecond
	offsetMus          = 800 * time.Millisecond
	offsetCountries    = 1000 * time.Millisecond
	offsetMuRefresh    = 1500 * time.Millisecond
	offsetParties      = 1600 * time.Millisecond
	offsetCompanies    = 2000 * time.Millisecond
	offsetUsers        = 2400 * time.Millisecond
	offsetPartyRefresh = 2500 * time.Millisecond
	offsetRegions      = 3200 * time.Millisecond
	offsetTradeOffers  = 4000 * time.Millisecond
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
	flusher := lastseen.New(colls, cfg.LastSeenInterval)
	muQueue := muqueue.New(colls, cfg.MuQueueBuffer, cfg.MuQueueInterval)
	partyQueue := partyqueue.New(colls, cfg.PartyQueueBuffer, cfg.PartyQueueInterval)
	muFlusher := lastseen.NewMuFlusher(colls, cfg.MuLastSeenInterval)
	partyFlusher := lastseen.NewPartyFlusher(colls, cfg.PartyLastSeenInterval)
	ingester := ingest.New(colls, queue, flusher, muQueue, partyQueue, muFlusher, partyFlusher)

	txScheduler := &scheduler.Transactions{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.TransactionsInterval,
		Offset:   offsetTransactions,
		Workers:  cfg.WorkerPoolSize,
	}
	usersScheduler := &scheduler.Users{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.UsersInterval,
		Offset:   offsetUsers,
		Workers:  cfg.WorkerPoolSize,
	}
	refreshScheduler := &scheduler.Refresh{
		Client:          client,
		Ingester:        ingester,
		Colls:           colls,
		Interval:        cfg.RefreshInterval,
		Offset:          offsetRefresh,
		Target:          cfg.RefreshTarget,
		RecentThreshold: cfg.LastSeenRecentThreshold,
	}
	regionsScheduler := &scheduler.Regions{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.RegionsInterval,
		Offset:   offsetRegions,
	}
	countriesScheduler := &scheduler.Countries{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.CountriesInterval,
		Offset:   offsetCountries,
	}
	companiesScheduler := &scheduler.Companies{
		Client:      client,
		Ingester:    ingester,
		Colls:       colls,
		Interval:    cfg.CompaniesInterval,
		Offset:      offsetCompanies,
		BackfillMax: cfg.CompaniesBackfillMax,
		Workers:     cfg.WorkerPoolSize,
	}
	battleRankingScheduler := &scheduler.BattleRanking{
		Client:      client,
		Ingester:    ingester,
		Colls:       colls,
		SweepPeriod: cfg.BattleRankingSweepPeriod,
	}
	tradeOffersScheduler := &scheduler.TradeOffers{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.TradeOffersInterval,
		Offset:   offsetTradeOffers,
		Limit:    cfg.TradeOffersLimit,
	}
	musScheduler := &scheduler.Mus{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.MuInterval,
		Offset:   offsetMus,
		Workers:  cfg.WorkerPoolSize,
	}
	muRefreshScheduler := &scheduler.MuRefresh{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.MuRefreshInterval,
		Offset:   offsetMuRefresh,
		Target:   cfg.MuRefreshTarget,
	}
	partiesScheduler := &scheduler.Parties{
		Client:   client,
		Ingester: ingester,
		Colls:    colls,
		Interval: cfg.PartyInterval,
		Offset:   offsetParties,
		Workers:  cfg.WorkerPoolSize,
	}
	partyRefreshScheduler := &scheduler.PartyRefresh{
		Client:       client,
		Ingester:     ingester,
		Colls:        colls,
		Interval:     cfg.PartyRefreshInterval,
		Offset:       offsetPartyRefresh,
		Target:       cfg.PartyRefreshTarget,
		RulingMaxAge: cfg.RulingPartyMaxAge,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return queue.Run(gctx) })
	g.Go(func() error { return flusher.Run(gctx) })
	g.Go(func() error { return muQueue.Run(gctx) })
	g.Go(func() error { return partyQueue.Run(gctx) })
	g.Go(func() error { return muFlusher.Run(gctx) })
	g.Go(func() error { return partyFlusher.Run(gctx) })
	g.Go(func() error { return txScheduler.Run(gctx) })
	g.Go(func() error { return usersScheduler.Run(gctx) })
	g.Go(func() error { return refreshScheduler.Run(gctx) })
	g.Go(func() error { return regionsScheduler.Run(gctx) })
	g.Go(func() error { return countriesScheduler.Run(gctx) })
	g.Go(func() error { return companiesScheduler.Run(gctx) })
	g.Go(func() error { return battleRankingScheduler.Run(gctx) })
	g.Go(func() error { return tradeOffersScheduler.Run(gctx) })
	g.Go(func() error { return musScheduler.Run(gctx) })
	g.Go(func() error { return muRefreshScheduler.Run(gctx) })
	g.Go(func() error { return partiesScheduler.Run(gctx) })
	g.Go(func() error { return partyRefreshScheduler.Run(gctx) })

	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Scraper exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("Scraper stopped")
}
