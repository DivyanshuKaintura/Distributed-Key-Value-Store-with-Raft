package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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

func main() {
	// ==================== STEP 1: Load Configuration ====================
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		log.Fatalf("[Startup] Failed to load configuration: %v", err)
	}
	log.Printf("[Startup] Configuration loaded for node: %s", cfg.NodeID)
	log.Printf("[Startup]   HTTP Port: %d, gRPC Port: %d", cfg.HTTPPort, cfg.GRPCPort)
	log.Printf("[Startup]   Data Dir: %s", cfg.DataDir)

	// Handle WAL recovery command
	if len(os.Args) > 1 && os.Args[1] == "recover" {
		err := RecoverFromWAL()
		if err != nil {
			log.Fatalf("[Startup] Failed to recover WAL: %v", err)
		}
		log.Println("[Startup] WAL recovered successfully")
		return
	}

	// ==================== STEP 2: Initialize Persistent Storage ====================
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("[Startup] Failed to create data directory: %v", err)
	}
	log.Printf("[Startup] Data directory ready: %s", cfg.DataDir)

	// ==================== STEP 3: Create RaftNode ====================
	applyCh := make(chan raft.ApplyMsg, 100)

	raftNode, err = raft.NewRaftNodeFromConfig(cfg, applyCh)
	if err != nil {
		log.Fatalf("[Startup] Failed to initialize Raft node: %v", err)
	}
	log.Printf("[Startup] Raft node initialized: %s", cfg.NodeID)

	// ==================== STEP 4: Start gRPC Server ====================
	grpcServer := raft.NewRaftGRPCServer(raftNode, cfg.GRPCPort)
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("[Startup] Failed to start gRPC server: %v", err)
	}
	log.Printf("[Startup] gRPC server started on port %d", cfg.GRPCPort)

	// ==================== STEP 5: Start Raft Main Loop ====================
	raftNode.Start()
	log.Printf("[Startup] Raft state machine started")

	// ==================== STEP 6: Start HTTP Server (with graceful shutdown) ====================
	httpAddr := fmt.Sprintf(":%d", cfg.HTTPPort)
	httpServer := setupHTTPServer(httpAddr)
	log.Printf("[Startup] HTTP server initialized on %s", httpAddr)

	// ==================== STEP 7: Start Apply Committed Entries Goroutine ====================
	go applyCommittedEntries(applyCh)
	log.Printf("[Startup] Committed entry applier started")

	// Load existing data from disk on startup
	LoadFromDisk()
	log.Printf("[Startup] Data loaded from disk")

	// ==================== STEP 8: Handle Graceful Shutdown ====================
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("[Startup] Initialization complete - waiting for requests")
		log.Println("Endpoints:")
		log.Printf("  Health Check: curl http://localhost%s/health", httpAddr)
		log.Printf("  Cluster Info: curl http://localhost%s/cluster", httpAddr)
		log.Printf("  PUT:          curl -X PUT -d 'value' http://localhost%s/kv/mykey", httpAddr)
		log.Printf("  GET:          curl http://localhost%s/kv/mykey", httpAddr)
		log.Printf("  DELETE:       curl -X DELETE http://localhost%s/kv/mykey", httpAddr)
		log.Println()

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[HTTP] Server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("[Shutdown] Received signal: %v", sig)

	// ==================== SHUTDOWN SEQUENCE ====================
	performGracefulShutdown(httpServer, grpcServer, raftNode, applyCh)
}

// setupHTTPServer creates and configures the HTTP server
func setupHTTPServer(addr string) *http.Server {
	mux := http.NewServeMux()

	// Apply timeout middleware to KV handler
	timeoutDuration := 5 * time.Second
	mux.HandleFunc("/kv/", TimeoutMiddleware(timeoutDuration)(kvHandler))

	// Health check endpoint
	mux.HandleFunc("/health", HealthCheckHandler)

	// Cluster info endpoint
	mux.HandleFunc("/cluster", clusterInfoHandler)

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// performGracefulShutdown handles the shutdown sequence gracefully
func performGracefulShutdown(httpServer *http.Server, grpcServer *raft.RaftGRPCServer, raftNode *raft.RaftNode, applyCh chan raft.ApplyMsg) {
	log.Println("[Shutdown] Starting graceful shutdown sequence...")

	// Step 1: Stop accepting new HTTP requests
	log.Println("[Shutdown] Stopping HTTP server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("[Shutdown] HTTP server shutdown error: %v", err)
	}
	log.Println("[Shutdown] HTTP server stopped")

	// Step 2: Stop accepting new gRPC requests
	log.Println("[Shutdown] Stopping gRPC server...")
	grpcServer.Stop()
	log.Println("[Shutdown] gRPC server stopped")

	// Step 3: Stop Raft timers and cleanup
	log.Println("[Shutdown] Stopping Raft state machine...")
	raftNode.Stop()
	log.Println("[Shutdown] Raft state machine stopped")

	// Step 3.5: Close apply channel so applier goroutine can exit
	if applyCh != nil {
		close(applyCh)
		log.Println("[Shutdown] applyCh closed")
	}

	// Step 4: Final data persistence
	log.Println("[Shutdown] Persisting final state...")
	if err := SaveToDisk(); err != nil {
		log.Printf("[Shutdown] Warning: failed to save final data: %v", err)
	}

	log.Println("[Shutdown] Graceful shutdown complete")
	os.Exit(0)
}

// applyCommittedEntries listens for committed entries and applies them to the KV store
func applyCommittedEntries(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}

		log.Printf("[Apply] Applying committed entry: index=%d, command=%s", msg.CommandIndex, msg.Command)

		// Parse command (format: "PUT|key|value" or "DELETE|key")
		parts := strings.SplitN(msg.Command, "|", 3)
		if len(parts) < 2 {
			log.Printf("[Apply] Invalid command format: %s", msg.Command)
			continue
		}

		operation := parts[0]
		key := parts[1]

		mu.Lock()
		switch operation {
		case "PUT":
			if len(parts) == 3 {
				value := parts[2]
				store[key] = value
				log.Printf("[Apply] PUT %s = %s", key, value)
			}
		case "DELETE":
			delete(store, key)
			log.Printf("[Apply] DELETE %s", key)
		default:
			log.Printf("[Apply] Unknown operation: %s", operation)
		}
		mu.Unlock()

		// Persist to disk after applying
		if err := SaveToDisk(); err != nil {
			log.Printf("[Apply] Warning: failed to save to disk: %v", err)
		}
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
