package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/vantutran2k1/aura/internal/router/parser"
	"github.com/vantutran2k1/aura/pkg/pprof"
)

const (
	natsAddress   = "nats://localhost:4222"
	rawLogSubject = "aura.raw.logs"
	numWorkers    = 50
	jobQueueSize  = 10000
	pprofPort     = "6062"
)

func main() {
	nc, err := nats.Connect(natsAddress)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("connected to NATS")

	go pprof.StartServer("localhost:" + pprofPort)

	workerPool := parser.NewWorkerPool(numWorkers, jobQueueSize, nc)
	workerPool.Start()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sub, err := nc.QueueSubscribe(rawLogSubject, "aura-router-group", func(msg *nats.Msg) {
		job := parser.Job{Msg: msg}
		workerPool.Submit(job)
	})
	if err != nil {
		log.Fatalf("faield to subscribe to NATS: %v", err)
	}
	log.Printf("subscribed to '%s' with queue group 'aura-router-group'", rawLogSubject)

	<-ctx.Done()

	log.Println("shutdown signal received, draining...")
	stop()

	if err := sub.Drain(); err != nil {
		log.Printf("error draining NATS subscription: %v", err)
	}

	workerPool.Stop()

	log.Println("aura-router service shut down gracefully")
}
