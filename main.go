package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// ============== In-Memory Store ==============

var store = make(map[string]string)
var mu sync.RWMutex

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

	if len(os.Args) > 1 && os.Args[1] == "recover" {
		err := RecoverFromWAL()
		if err != nil {
			log.Fatalf("Failed to Recover WAL: %v", err)
		}
		log.Println("WAL recovered successfully")
		return
	}

	// Load existing data from disk on startup
	LoadFromDisk()

	// Automatic crash recovery
	// Replay WAL to recover any operations since last save
	// ReplayWAL()

	// Apply timeout middleware to KV handler
	// Set timeout to 5 seconds
	// For distributed systems, this should be < than client retry timeout
	timeoutDuration := 5 * time.Second
	http.HandleFunc("/kv/", TimeoutMiddleware(timeoutDuration)(kvHandler))

	// Health check endpoint - used by load balancers, k8s, monitoring
	http.HandleFunc("/health", HealthCheckHandler)

	log.Println("KV Store running on http://localhost:8080")
	log.Println("Request timeout set to:", timeoutDuration)
	log.Println("Endpoints:")
	log.Println("  Health Check: curl http://localhost:8080/health")
	log.Println("  PUT:          curl -X PUT -d 'value' http://localhost:8080/kv/mykey")
	log.Println("  GET:          curl http://localhost:8080/kv/mykey")
	log.Println("  DELETE:       curl -X DELETE http://localhost:8080/kv/mykey")
	http.ListenAndServe(":8080", nil)
}
