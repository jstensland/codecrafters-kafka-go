package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close() // Ensure connection is closed when handler exits

	// Incoming request:
	// 00 00 00 23  // message_size:        35
	// 00 12        // request_api_key:     18
	// 00 04        // request_api_version: 4
	// 6f 7f c6 61  // correlation_id:      1870644833

	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBytes); err != nil {
		if err != io.EOF { // EOF might be okay if client just connects and disconnects
			fmt.Println("Error reading message size:", err.Error())
		}
		return
	}
	fmt.Println("sizeBytes:", sizeBytes)
	messageSize := binary.BigEndian.Uint32(sizeBytes)
	fmt.Println("Message size:", messageSize)

	// 2. Read the rest of the message (messageSize bytes)
	// The total message includes the size prefix, correlation id, etc.
	// We already read 4 bytes for the size, so we need to read messageSize more bytes
	// to get the rest of the request payload which includes the correlation ID.
	// Note: Kafka protocol's message_size includes the size field itself.
	// The total request size is 4 + messageSize. We read 4 already.
	// The remaining payload size is messageSize.
	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, payload); err != nil {
		fmt.Println("Error reading message payload:", err.Error())
		return
	}

	fmt.Println("payload:", payload)
	// The full message buffer (excluding the size prefix we already read) is in `payload`.
	// The correlation ID starts at byte 4 within this payload (since it's bytes 8-11 of the *total* message).
	if len(payload) < 8 { // Ensure payload is large enough for correlation ID (ApiKey+ApiVersion+CorrelationID)
		fmt.Println("Error: Payload too small for correlation ID")
		return
	}
	correlationID := binary.BigEndian.Uint32(payload[4:8]) // Extract correlation ID (bytes 4-7 of payload)

	// Construct the response: Size (int32) = 4, Correlation ID (int32) = extracted correlation ID
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 4)             // Size = 4 bytes
	binary.BigEndian.PutUint32(response[4:8], correlationID) // Use extracted Correlation ID

	_, err := conn.Write(response) // Use := to declare err
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
