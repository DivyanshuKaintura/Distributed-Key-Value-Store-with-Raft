package main

import (
	"fmt"
	"log"
	"net/http"
)

func mainHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!")
}

func main() {
	http.HandleFunc("/", mainHandler)
	log.Println("Server running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
