package traces_parser

import (
	"context"
	"encoding/hex"
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
	processedSubject = "aura.processed.traces"
)

type Value struct {
	StringValue string `json:"stringValue"`
}

type Attribute struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

type Span struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	ParentSpanID      string      `json:"parentSpanId"`
	Name              string      `json:"name"`
	StartTimeUnixNano string      `json:"startTimeUnixNano"`
	EndTimeUnixNano   string      `json:"endTimeUnixNano"`
	Attributes        []Attribute `json:"attributes"`
}

type ScopeSpan struct {
	Spans []Span `json:"spans"`
}

type Resource struct {
	Attributes []Attribute `json:"attributes"`
}

type ResourceSpan struct {
	Resource   Resource    `json:"resource"`
	ScopeSpans []ScopeSpan `json:"scopeSpans"`
}

type OTLPPayload struct {
	ResourceSpans []ResourceSpan `json:"resourceSpans"`
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
	log.Printf("[traces] started %d workers", wp.numWorkers)
}

func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
	log.Println("[traces] all workers stopped.")
}

func (wp *WorkerPool) Submit(job Job) {
	wp.jobs <- job
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobs {
		workerCtx, span := wp.tracer.Start(job.Ctx, "trace-router-worker")

		var payload OTLPPayload
		if err := json.Unmarshal(job.Msg.Data, &payload); err != nil {
			log.Printf("[traces] worker %d: error unmarshaling json: %v", id, err)
			span.End()
			continue
		}

		for _, rs := range payload.ResourceSpans {
			resAttrs := attributesToMap(rs.Resource.Attributes)

			for _, ss := range rs.ScopeSpans {
				for _, otlpSpan := range ss.Spans {
					pbSpan, err := wp.convertSpan(otlpSpan, resAttrs)
					if err != nil {
						log.Printf("[traces] worker %d: error converting span: %v", id, err)
						continue
					}

					if err := wp.publishSpan(workerCtx, pbSpan); err != nil {
						log.Printf("[traces] worker %d: %v", id, err)
					}
				}
			}
		}
		span.End()
	}
}

func (wp *WorkerPool) convertSpan(otlpSpan Span, resAttrs map[string]string) (*pb.Span, error) {
	traceID, err := hex.DecodeString(otlpSpan.TraceID)
	if err != nil {
		return nil, fmt.Errorf("invalid traceId: %w", err)
	}
	spanID, err := hex.DecodeString(otlpSpan.SpanID)
	if err != nil {
		return nil, fmt.Errorf("invalid spanId: %w", err)
	}
	parentSpanID, _ := hex.DecodeString(otlpSpan.ParentSpanID)

	pbSpan := &pb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		Name:              otlpSpan.Name,
		StartTimeUnixNano: mustParseNano(otlpSpan.StartTimeUnixNano),
		EndTimeUnixNano:   mustParseNano(otlpSpan.EndTimeUnixNano),
		Attributes:        resAttrs,
	}

	for k, v := range attributesToMap(otlpSpan.Attributes) {
		pbSpan.Attributes[k] = v
	}

	return pbSpan, nil
}

func (wp *WorkerPool) publishSpan(ctx context.Context, span *pb.Span) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled, skipping publish: %w", err)
	}

	data, err := proto.Marshal(span)
	if err != nil {
		return fmt.Errorf("failed to marshal span: %w", err)
	}

	if err := wp.nc.Publish(processedSubject, data); err != nil {
		return fmt.Errorf("failed to publish span: %w", err)
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
	i, _ := strconv.ParseInt(ts, 10, 64)
	if i == 0 {
		t, _ := time.Parse(time.RFC3339Nano, ts)
		return t.UnixNano()
	}
	return i
}
