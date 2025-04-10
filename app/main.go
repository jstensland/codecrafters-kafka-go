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

	l, err := net.Listen("tcp", "0.0.0.0:9092")
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

	// Response: Size (int32) = 4, Correlation ID (int32) = 7
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 4) // Size = 4 bytes
	binary.BigEndian.PutUint32(response[4:8], 7) // Correlation ID = 7

	_, err := conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
	}
}
