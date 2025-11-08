package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

const (
	cacheExpiration = 30 * time.Second
)

type APIHandler struct {
	storageClient pb.StorageServiceClient
	redisClient   *redis.Client
}

func NewAPIHandler(sc pb.StorageServiceClient, rc *redis.Client) *APIHandler {
	return &APIHandler{
		storageClient: sc,
		redisClient:   rc,
	}
}

func (h *APIHandler) HandleLogsQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	query := r.URL.Query().Get("q")
	limitStr := r.URL.Query().Get("limit")
	// TODO: add start and end time

	limit, err := strconv.ParseUint(limitStr, 10, 32)
	if err != nil || limit == 0 {
		limit = 100
	}

	cacheKey := fmt.Sprintf("query:logs:q=%s:limit=%d", query, limit)
	cachedResult, err := h.redisClient.Get(ctx, cacheKey).Bytes()
	if err == nil {
		log.Println("cache hit")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Aura-Cache", "HIT")
		w.Write(cachedResult)
		return
	}

	log.Println("cache miss")

	grpcReq := &pb.QueryLogsRequest{
		Query: query,
		Limit: uint32(limit),
		// Use wide time range for now
		StartTimeUnixNano: 1,
		EndTimeUnixNano:   time.Now().Add(1 * time.Hour).UnixNano(),
	}

	grpcRes, err := h.storageClient.QueryLogs(ctx, grpcReq)
	if err != nil {
		log.Printf("gRPC client error: %v", err)
		http.Error(w, "error querying storage", http.StatusInternalServerError)
		return
	}

	jsonResponse, err := json.Marshal(grpcRes)
	if err != nil {
		http.Error(w, "error serializing response", http.StatusInternalServerError)
		return
	}

	if err := h.redisClient.Set(ctx, cacheKey, jsonResponse, cacheExpiration).Err(); err != nil {
		log.Printf("faield to set cache: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Aura-Cache", "MISS")
	w.Write(jsonResponse)
}
