package http

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"google.golang.org/protobuf/proto"
)

const (
	natsSubject = "aura.raw.logs"
)

var logPool = sync.Pool{
	New: func() interface{} {
		return &pb.Log{}
	},
}

type APIHandler struct {
	nc *nats.Conn
}

func NewAPIHandler(nc *nats.Conn) *APIHandler {
	return &APIHandler{nc: nc}
}

func (h *APIHandler) HandleLogs(w http.ResponseWriter, r *http.Request) {
	logMsg := logPool.Get().(*pb.Log)

	if err := json.NewDecoder(r.Body).Decode(logMsg); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

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

	if err := h.nc.Publish(natsSubject, data); err != nil {
		log.Printf("failed to publish to NATS: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	logMsg.Reset()
	logPool.Put(logMsg)

	w.WriteHeader(http.StatusAccepted)
}
