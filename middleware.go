package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

// ============== Timeout Middleware ==============

// TimeoutMiddleware wraps an HTTP handler and enforces a timeout on the request.
// If the handler takes longer than the specified duration, it returns 408 Request Timeout.
//
// Why we need this:
// - Prevents clients from waiting forever on slow operations
// - Protects server from hanging requests consuming resources
// - Critical for distributed systems to fail fast
//
// How it works:
// 1. Creates a context with timeout using context.WithTimeout
// 2. Passes this context to the handler via r.WithContext()
// 3. If handler exceeds timeout, context.Done() channel is closed
// 4. We detect this and return 408 to client
func TimeoutMiddleware(timeout time.Duration) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// Create a context with timeout
			// This context will automatically cancel after 'timeout' duration
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel() // IMPORTANT: Always defer cancel to free resources

			// Create a channel to signal when handler completes
			done := make(chan bool, 1)

			// Store original response writer to check if we already responded
			var handlerPanicked bool

			// Run the handler in a goroutine so we can race it against the timeout
			go func() {
				defer func() {
					if err := recover(); err != nil {
						// Handler panicked, log it
						log.Printf("Handler panicked: %v", err)
						handlerPanicked = true
					}
					done <- true // Signal completion
				}()

				// Call the actual handler with the timeout context
				// The handler can check ctx.Done() to see if timeout occurred
				next(w, r.WithContext(ctx))
			}()

			// Wait for either:
			// 1. Handler to complete (done channel)
			// 2. Timeout to occur (ctx.Done() channel)
			select {
			case <-done:
				// Handler completed in time
				if handlerPanicked {
					// Handler panicked, return 500
					SendError(w, ErrInternalServer, "internal server error", "", "handler panicked")
				}
				// Otherwise, handler already wrote response, we're done
				return

			case <-ctx.Done():
				// Timeout occurred!
				// The context was cancelled due to timeout
				log.Printf("Request timeout for %s %s", r.Method, r.URL.Path)

				// Return 408 Request Timeout with structured error
				SendError(w, ErrRequestTimeout, "request exceeded timeout limit", "",
					fmt.Sprintf("timeout: %v", timeout))
				return
			}
		}
	}
}

// ============== Context-Aware Operations ==============

// Example: How to make your handlers respect the timeout context
// Your handler should periodically check if context is cancelled
func ExampleContextAwareOperation(ctx context.Context) error {
	// Before doing expensive work, check if context is still valid
	select {
	case <-ctx.Done():
		// Context was cancelled (timeout or client disconnect)
		return ctx.Err() // Returns context.DeadlineExceeded or context.Canceled
	default:
		// Context still valid, continue work
	}

	// Do your work here...
	// For long operations, check ctx.Done() in a loop

	return nil
}
