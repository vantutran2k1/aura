package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go"
	aurahttp "github.com/vantutran2k1/aura/internal/ingestor/http"
)

const (
	natsAddress = "nats://localhost:4222"
	apiPort     = "8080"
)

func main() {
	nc, err := nats.Connect(natsAddress)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("connected to NATS")

	apiHandler := aurahttp.NewAPIHandler(nc)

	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)

	r.Post("/v1/logs", apiHandler.HandleLogs)

	srv := &http.Server{
		Addr:    ":" + apiPort,
		Handler: r,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("aura-ingestor starting on port %s", apiPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	nc.Drain()

	log.Printf("aura-ingestor service shut down gracefully")
}
