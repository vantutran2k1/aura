package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"
)

const (
	natsAddress   = "nats://localhost:4222"
	rawLogSubject = "aura.raw.logs"
	numWorkers    = 50
	jobQueueSize  = 10000
	pprofPort     = "6062"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := newApp(ctx)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}

	go func() {
		if err := app.run(ctx); err != nil {
			log.Printf("app run error: %v", err)
		}
	}()

	appErrCh := make(chan error, 1)
	go func() {
		appErrCh <- app.run(ctx)
	}()

	select {
	case err := <-appErrCh:
		if err != nil {
			log.Printf("app run error: %v", err)
		}
	case <-ctx.Done():
		log.Println("shutdown signal received")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := app.shutdown(shutdownCtx); err != nil {
		log.Printf("app shutdown error: %v", err)
	}
}
