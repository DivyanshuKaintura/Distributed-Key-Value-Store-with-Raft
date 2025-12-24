package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/config"
	"github.com/DivyanshuKaintura/Distributed-Key-Value-Store-with-Raft/raft"
)

// ============== In-Memory Store ==============

var store = make(map[string]string)
var mu sync.RWMutex

// Global Raft node and config (will be initialized in main)
var raftNode *raft.RaftNode
var cfg *config.Config

// ============== Response Helpers ==============

type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
}

// ============== KV Handler ==============

// kvHandler handles all /kv/{key} requests
// GET    /kv/{key}         - retrieve value
// PUT    /kv/{key}         - store value (body = value)
// DELETE /kv/{key}         - delete key
func kvHandler(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path: /kv/{key}
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	log.Println("Received", r.Method, "request for key:", key)

	if key == "" {
		SendJSON(w, http.StatusBadRequest, Response{
			Success: false,
			Message: "key is required in path: /kv/{key}",
		})
		return
	}

	switch r.Method {
	case http.MethodGet:
		HandleGet(w, key)
	case http.MethodPut:
		HandlePut(w, r, key)
	case http.MethodDelete:
		HandleDelete(w, key)
	default:
		SendJSON(w, http.StatusMethodNotAllowed, Response{
			Success: false,
			Message: "method not allowed, use GET, PUT, or DELETE",
		})
	}
}

// ============== Main ==============

func main() {
	// Load configuration
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	log.Printf("Configuration loaded: %s", cfg)

	// Handle WAL recovery command
	if len(os.Args) > 1 && os.Args[1] == "recover" {
		err := RecoverFromWAL()
		if err != nil {
			log.Fatalf("Failed to Recover WAL: %v", err)
		}
		log.Println("WAL recovered successfully")
		return
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize Raft node
	raftNode = raft.NewRaftNode(cfg.NodeID)
	log.Printf("Raft node initialized: %s", cfg.NodeID)

	// Start gRPC server for peer communication
	grpcServer := raft.NewRaftGRPCServer(raftNode, cfg.GRPCPort)
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
	defer grpcServer.Stop()

	// Load existing data from disk on startup
	LoadFromDisk()

	// Apply timeout middleware to KV handler
	timeoutDuration := 5 * time.Second
	http.HandleFunc("/kv/", TimeoutMiddleware(timeoutDuration)(kvHandler))

	// Health check endpoint - used by load balancers, k8s, monitoring
	http.HandleFunc("/health", HealthCheckHandler)

	// Cluster info endpoint
	http.HandleFunc("/cluster", clusterInfoHandler)

	httpAddr := fmt.Sprintf(":%d", cfg.HTTPPort)
	log.Printf("KV Store running on http://localhost%s", httpAddr)
	log.Printf("gRPC Server running on port %d", cfg.GRPCPort)
	log.Println("Request timeout set to:", timeoutDuration)
	log.Println("Endpoints:")
	log.Printf("  Health Check: curl http://localhost%s/health", httpAddr)
	log.Printf("  Cluster Info: curl http://localhost%s/cluster", httpAddr)
	log.Printf("  PUT:          curl -X PUT -d 'value' http://localhost%s/kv/mykey", httpAddr)
	log.Printf("  GET:          curl http://localhost%s/kv/mykey", httpAddr)
	log.Printf("  DELETE:       curl -X DELETE http://localhost%s/kv/mykey", httpAddr)

	if err := http.ListenAndServe(httpAddr, nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// clusterInfoHandler returns information about the cluster
func clusterInfoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		SendJSON(w, http.StatusMethodNotAllowed, Response{
			Success: false,
			Message: "method not allowed",
		})
		return
	}

	info := map[string]interface{}{
		"node_id":   cfg.NodeID,
		"state":     raftNode.GetState().String(),
		"leader_id": raftNode.GetLeaderID(),
		"term":      raftNode.GetCurrentTerm(),
		"peers":     cfg.Peers,
		"http_port": cfg.HTTPPort,
		"grpc_port": cfg.GRPCPort,
	}

	SendJSONAny(w, http.StatusOK, info)
}
