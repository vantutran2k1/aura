package http

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"google.golang.org/protobuf/proto"
)

var logPool = sync.Pool{
	New: func() interface{} {
		return &pb.Log{}
	},
}

type APIHandler struct {
	nc             *nats.Conn
	logsSubject    string
	metricsSubject string
	tracesSubject  string
}

func NewAPIHandler(nc *nats.Conn, logsSubject, metricsSubject, tracesSubject string) *APIHandler {
	return &APIHandler{
		nc:             nc,
		logsSubject:    logsSubject,
		metricsSubject: metricsSubject,
		tracesSubject:  tracesSubject,
	}
}

func (h *APIHandler) HandleLogs(w http.ResponseWriter, r *http.Request) {
	logMsg := logPool.Get().(*pb.Log)
	if err := json.NewDecoder(r.Body).Decode(logMsg); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	logMsg.Id = uuid.NewString()
	logMsg.TimestampUnixNano = time.Now().UnixNano()
	if logMsg.ServiceName == "" {
		http.Error(w, "missing 'service_name'", http.StatusBadRequest)
		logMsg.Reset()
		logPool.Put(logMsg)
		return
	}

	data, err := proto.Marshal(logMsg)
	if err != nil {
		log.Printf("failed to marshal log: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := h.nc.Publish(h.logsSubject, data); err != nil {
		log.Printf("failed to publish to NATS: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	logMsg.Reset()
	logPool.Put(logMsg)
	w.WriteHeader(http.StatusAccepted)
}

func (h *APIHandler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("error reading metrics body: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	if err := h.nc.Publish(h.metricsSubject, body); err != nil {
		log.Printf("failed to publish metrics to nats: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *APIHandler) HandleTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[traces] error reading body: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	if err := h.nc.Publish(h.tracesSubject, body); err != nil {
		log.Printf("[traces] failed to publish to nats: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
