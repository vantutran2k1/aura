package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := newApp(ctx)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}

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
