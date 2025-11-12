package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"
)

const (
	storageGRPCAddress = "localhost:50051"
	redisAddress       = "localhost:6379"
	apiPort            = "8081"
	pprofPort          = "6061"
	metricsPort        = "9092"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := newApp(ctx)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}

	go func() {
		if err := app.run(); err != nil {
			log.Printf("app run error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := app.shutdown(shutdownCtx); err != nil {
		log.Printf("app shutdown error: %v", err)
	}
}
