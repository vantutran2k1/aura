package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"github.com/vantutran2k1/aura/internal/alerter"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	pprofPort          string
	postgresURL        string
	storageGRPCAddress string
	schedulerInterval  time.Duration
}

type app struct {
	config    config
	tp        *sdktrace.TracerProvider
	db        *pgxpool.Pool
	grpcConn  *grpc.ClientConn
	scheduler *alerter.Scheduler
}

func newApp(ctx context.Context) (*app, error) {
	cfg := config{
		pprofPort:          "6064",
		postgresURL:        "postgres://user:password@localhost:5432/aura",
		storageGRPCAddress: "localhost:50051",
		schedulerInterval:  10 * time.Second,
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-alerter")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}

	db, err := pgxpool.New(ctx, cfg.postgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	if err := db.Ping(ctx); err != nil {
		return nil, fmt.Errorf("faield to ping postgres: %w", err)
	}
	log.Println("connected to postgresql")

	grpcConn, err := grpc.NewClient(
		cfg.storageGRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to grpc server: %w", err)
	}
	storageClient := pb.NewStorageServiceClient(grpcConn)

	ruleStore := alerter.NewRuleStore(db)
	notifier := alerter.NewWebhookNotifier()
	scheduler := alerter.NewScheduler(ruleStore, notifier, storageClient, cfg.schedulerInterval)

	go pprof.StartServer("localhost:" + cfg.pprofPort)

	return &app{
		config:    cfg,
		tp:        tp,
		db:        db,
		grpcConn:  grpcConn,
		scheduler: scheduler,
	}, nil
}

func (a *app) run(ctx context.Context) error {
	log.Println("aura-alerter scheduler starting...")
	return a.scheduler.Run(ctx)
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down alerter...")

	a.db.Close()
	a.grpcConn.Close()

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-alerter shutdown gracefully")
	return nil
}
