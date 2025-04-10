package main

import (
	"encoding/binary"
	"fmt"
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

	// Read the request header (first 12 bytes)
	header := make([]byte, 12)
	_, err := conn.Read(header)
	if err != nil {
		fmt.Println("Error reading request header:", err.Error())
		return // Stop processing if we can't read the header
	}

	// Extract the correlation ID (bytes 8-11)
	correlationID := binary.BigEndian.Uint32(header[8:12])

	// Construct the response
	// Response: Size (int32) = 4, Correlation ID (int32) = extracted correlation ID
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 4)             // Size = 4 bytes
	binary.BigEndian.PutUint32(response[4:8], correlationID) // Use extracted Correlation ID

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
