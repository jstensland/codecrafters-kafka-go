// Package server implements the Kafka broker server functionality.
package server

import (
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

// Serve starts a Kafka broker server that listens on port 9092 and handles client connections.
func Serve() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log.Println("Logs from your program will appear here!")

	// Listen on both IPv4 and IPv6
	// #nosec G102 -- This is intentional for the challenge
	l, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}() // Ensure listener is closed when main exits

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue // Continue listening for other connections
		}

		// Handle connection in a new goroutine to allow concurrent connections
		go handlers.HandleConnection(conn, handlers.ConnectionReadTimeout)
	}
}
