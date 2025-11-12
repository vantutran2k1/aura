package parser

import (
	"context"
	"log"
	"strconv"
	"sync"

	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const (
	processedSubject = "aura.processed.logs"
)

type Job struct {
	Msg *nats.Msg
	Ctx context.Context
}

type WorkerPool struct {
	numWorkers int
	jobs       chan Job
	wg         sync.WaitGroup
	nc         *nats.Conn
	tracer     trace.Tracer
}

func NewWorkerPool(
	numWorkers int,
	jobQueueSize int,
	nc *nats.Conn,
	tracer trace.Tracer,
) *WorkerPool {
	wp := &WorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan Job, jobQueueSize),
		nc:         nc,
		tracer:     tracer,
	}
	return wp
}

func (wp *WorkerPool) Start() {
	for i := 1; i <= wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	log.Printf("started %d workers", wp.numWorkers)
}

func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
	log.Println("all workers stopped")
}

func (wp *WorkerPool) Submit(job Job) {
	wp.jobs <- job
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobs {
		workerCtx, span := wp.tracer.Start(job.Ctx, "aura-router-worker")

		var logMsg pb.Log
		if err := proto.Unmarshal(job.Msg.Data, &logMsg); err != nil {
			log.Printf("worker %d: error unmarshaling log: %v\n", id, err)
			span.End()
			continue
		}

		if logMsg.Attributes == nil {
			logMsg.Attributes = make(map[string]string)
		}
		logMsg.Attributes["processed_by"] = "aura-router"
		logMsg.Attributes["worker_id"] = strconv.Itoa(id)

		processedData, err := proto.Marshal(&logMsg)
		if err != nil {
			log.Printf("worker %d: error marshiling processed log: %s\n", id, err)
			span.End()
			continue
		}

		if err := workerCtx.Err(); err != nil {
			log.Printf("worker %d: context canceled, skipping publish: %v", id, err)
			span.End()
			continue
		}

		if err := wp.nc.Publish(processedSubject, processedData); err != nil {
			log.Printf("worker %d: error publishing processed log: %v\n", id, err)
			// TODO: add retry logic
		}

		span.End()
	}

}
