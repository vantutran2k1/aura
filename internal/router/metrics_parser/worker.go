package metrics_parser

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const (
	processedSubject = "aura.processed.metrics"
)

type Value struct {
	StringValue string `json:"stringValue"`
}

type Attribute struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

type DataPoint struct {
	TimeUnixNano string      `json:"timeUnixNano"`
	AsDouble     float64     `json:"asDouble"`
	AsInt        string      `json:"asInt"`
	Attributes   []Attribute `json:"attributes"`
}

type Sum struct {
	DataPoints []DataPoint `json:"dataPoints"`
}

type Gauge struct {
	DataPoints []DataPoint `json:"dataPoints"`
}

type Resource struct {
	Attributes []Attribute `json:"attributes"`
}

type Metric struct {
	Name  string `json:"name"`
	Sum   *Sum   `json:"sum"`
	Gauge *Gauge `json:"gauge"`
}

type ScopeMetric struct {
	Metrics []Metric `json:"metrics"`
}

type ResourceMetric struct {
	Resource     Resource      `json:"resource"`
	ScopeMetrics []ScopeMetric `json:"scopeMetrics"`
}

type OTLPPayload struct {
	ResourceMetrics []ResourceMetric `json:"resourceMetrics"`
}

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

func NewWorkerPool(numWorkers int, jobQueueSize int, nc *nats.Conn, tracer trace.Tracer) *WorkerPool {
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
	log.Printf("[metrics] started %d workers", wp.numWorkers)
}

func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
	log.Println("[metrics] all workers stopped")
}

func (wp *WorkerPool) Submit(job Job) {
	wp.jobs <- job
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobs {
		workerCtx, span := wp.tracer.Start(job.Ctx, "metrics-router-worker")

		var payload OTLPPayload
		if err := json.Unmarshal(job.Msg.Data, &payload); err != nil {
			log.Printf("[metrics] worker %d: error unmarshaling JSON: %v", id, err)
			span.End()
			continue
		}

		for _, rm := range payload.ResourceMetrics {
			resAttrs := attributesToMap(rm.Resource.Attributes)

			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					var dataPoints []DataPoint
					var isCounter bool

					if m.Sum != nil {
						dataPoints = m.Sum.DataPoints
						isCounter = true
					} else if m.Gauge != nil {
						dataPoints = m.Gauge.DataPoints
					} else {
						continue
					}

					for _, dp := range dataPoints {
						pbMetric := &pb.Metric{
							Name:              m.Name,
							TimestampUnixNano: mustParseNano(dp.TimeUnixNano),
							Attributes:        resAttrs,
						}

						for k, v := range attributesToMap(dp.Attributes) {
							pbMetric.Attributes[k] = v
						}

						if isCounter {
							pbMetric.DataPoint = &pb.Metric_Counter{
								Counter: &pb.Counter{Total: mustParseInt64(dp.AsInt)},
							}
						} else {
							pbMetric.DataPoint = &pb.Metric_Gauge{
								Gauge: &pb.Gauge{Value: dp.AsDouble},
							}
						}

						if err := wp.publishMetric(workerCtx, pbMetric); err != nil {
							log.Printf("[metrics] worker %d: %v", id, err)
						}
					}
				}
			}
		}
	}
}

func (wp *WorkerPool) publishMetric(ctx context.Context, metric *pb.Metric) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled, skipping publish: %w", err)
	}

	data, err := proto.Marshal(metric)
	if err != nil {
		return fmt.Errorf("failed to marshal metric: %w", err)
	}

	if err := wp.nc.Publish(processedSubject, data); err != nil {
		return fmt.Errorf("faield to publish metric: %w", err)
	}

	return nil
}

func attributesToMap(attrs []Attribute) map[string]string {
	m := make(map[string]string)
	for _, a := range attrs {
		m[a.Key] = a.Value.StringValue
	}
	return m
}

func mustParseNano(ts string) int64 {
	t, _ := time.Parse(time.RFC3339Nano, ts)
	if t.IsZero() {
		i, _ := strconv.ParseInt(ts, 10, 64)
		return i
	}
	return t.UnixNano()
}

func mustParseInt64(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}
