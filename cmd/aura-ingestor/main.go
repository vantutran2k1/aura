package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"
)

const (
	natsAddress = "nats://localhost:4222"
	apiPort     = "8080"
	pprofPort   = "6060"
	metricsPort = "9091"
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

	shutdownCtx, shutdownCancle := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancle()

	if err := app.shutdown(shutdownCtx); err != nil {
		log.Printf("app shutdown error: %v", err)
	}
}
