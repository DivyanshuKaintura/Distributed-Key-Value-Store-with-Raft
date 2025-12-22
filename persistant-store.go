package main

import (
	"encoding/json"
	"log"
	"os"
)

const dataFile = "data.json"

// SaveToDisk saves the store to a JSON file
func SaveToDisk() error {
	mu.RLock()
	defer mu.RUnlock()

	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(dataFile, data, 0644)
	if err != nil {
		return err
	}

	log.Println("Data saved to", dataFile)
	return nil
}

// LoadFromDisk loads the store from a JSON file
func LoadFromDisk() {
	data, err := os.ReadFile(dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("No existing data file, starting fresh")
			return
		}
		log.Println("Error reading data file:", err)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	err = json.Unmarshal(data, &store)
	if err != nil {
		log.Println("Error parsing data file:", err)
		return
	}

	log.Println("Loaded", len(store), "keys from", dataFile)
}
