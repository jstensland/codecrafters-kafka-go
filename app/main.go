package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Request represents the structure of an incoming Kafka request header
type Request struct {
	Size           uint32
	ApiKey         uint16 // Not used yet, but part of the header
	ApiVersion     uint16 // Not used yet, but part of the header
	CorrelationID  uint32
	RemainingBytes []byte // The rest of the payload after the header fields
}

// ParseRequest reads from the reader and parses the Kafka request header
func ParseRequest(reader io.Reader) (*Request, error) {
	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, sizeBytes); err != nil {
		// Distinguish EOF from other errors for cleaner handling
		if err == io.EOF {
			return nil, io.EOF // Indicate clean connection closure
		}
		return nil, fmt.Errorf("reading message size: %w", err)
	}
	messageSize := binary.BigEndian.Uint32(sizeBytes)

	// Check for reasonable message size to prevent huge allocations
	// (Adjust limit as needed)
	if messageSize > 1024*1024 { // Example limit: 1MB
		return nil, fmt.Errorf("message size %d exceeds limit", messageSize)
	}
	if messageSize < 8 { // Minimum size for ApiKey, ApiVersion, CorrelationID
		return nil, fmt.Errorf("message size %d is too small for header", messageSize)
	}

	// 2. Read the rest of the message payload (messageSize bytes)
	// The payload contains ApiKey, ApiVersion, CorrelationID, etc.
	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("reading message payload: %w", err)
	}

	// 3. Parse fields from the payload
	req := &Request{
		Size:           messageSize,
		ApiKey:         binary.BigEndian.Uint16(payload[0:2]),
		ApiVersion:     binary.BigEndian.Uint16(payload[2:4]),
		CorrelationID:  binary.BigEndian.Uint32(payload[4:8]),
		RemainingBytes: payload[8:], // Store the rest if needed later
	}

	return req, nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Listen on both IPv4 and IPv6
	l, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close() // Ensure listener is closed when main exits

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue // Continue listening for other connections
		}

		// Handle connection in a new goroutine to allow concurrent connections
		go HandleConnection(conn)
	}
}

// HandleConnection processes a single client connection
func HandleConnection(conn net.Conn) {
	defer conn.Close() // Ensure connection is closed when handler exits

	// Parse the incoming request
	req, err := ParseRequest(conn)
	if err != nil {
		// Handle EOF separately, client might just disconnect
		if err == io.EOF {
			fmt.Println("Client disconnected gracefully.")
		} else {
			fmt.Println("Error parsing request:", err.Error())
		}
		return // Stop processing on error
	}

	// Construct the response using the parsed Correlation ID
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 4)                 // Size = 4 bytes
	binary.BigEndian.PutUint32(response[4:8], req.CorrelationID) // Use extracted Correlation ID

	// Send the response
	_, writeErr := conn.Write(response)
	if writeErr != nil {
		fmt.Println("Error writing response:", writeErr.Error())
	}
}
