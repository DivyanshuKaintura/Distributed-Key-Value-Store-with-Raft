package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

const wal = "wal.log"

func WriteAheadLog(operation string, key string, value string) error {
	// create or append to the log file
	walFile, err := os.OpenFile(wal, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// defer makes sure the file is closed after writing even if an error occurs
	defer walFile.Close()

	logEntry := operation + "|" + key + "|" + value + "\n"

	_, err = walFile.WriteString(logEntry)
	if err != nil {
		return err
	}

	// ensure the log entry is flushed to disk
	walFile.Sync()
	log.Printf("WAL: %s -> %s = %s", operation, key, value)
	return nil
}

func ReplayWAL() error {
	walFile, err := os.Open(wal)
	if err != nil {
		log.Printf("WAL: error-> %s", err)
		return err
	}
	defer walFile.Close()

	scanner := bufio.NewScanner(walFile)
	count := 0

	mu.Lock()
	defer mu.Unlock()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "|", 3)
		if len(parts) < 2 {
			continue
		}

		operation := parts[0]
		key := parts[1]
		value := ""
		if len(parts) == 3 {
			value = parts[2]
		}

		switch operation {
		case "PUT":
			store[key] = value
		case "DELETE":
			delete(store, key)
		default:
			log.Printf("WAL: unknown operation %s", operation)
		}
		count++
	}

	log.Printf("WAL: replayed %d entries", count)
	return nil
}

func ClearWAL() error {
	err := os.Remove(wal)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	log.Println("WAL: cleared")
	return nil
}

func RecoverFromWAL() error {
	log.Println("Starting recovery from WAL...")
	err := ReplayWAL()
	SaveToDisk()
	if err != nil {
		return err
	}
	return nil
}
