//nolint:err113,wrapcheck // dynamic and passed errors in tests are okay
package server_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	// Added
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

var ErrClosingMockConn = errors.New("errors closing mock connection") // Added

// mockListener implements the net.Listener interface for testing
type mockListener struct {
	connections []net.Conn
	acceptErr   error
	closed      bool
	addr        net.Addr
}

func (m *mockListener) Accept() (net.Conn, error) {
	if m.closed {
		return nil, net.ErrClosed
	}
	if m.acceptErr != nil {
		return nil, m.acceptErr
	}
	if len(m.connections) == 0 {
		return nil, errors.New("no more connections")
	}
	conn := m.connections[0]
	m.connections = m.connections[1:]
	return conn, nil
}

func (m *mockListener) Close() error {
	m.closed = true
	return nil
}

func (m *mockListener) Addr() net.Addr {
	return m.addr
}

// mockAddr implements the net.Addr interface for testing
type mockAddr struct{}

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return "127.0.0.1:9092" }

// mockConn implements the net.Conn interface for testing purposes.
type mockConn struct {
	reader     io.Reader
	writer     io.Writer
	closed     bool
	localAddr  net.Addr
	remoteAddr net.Addr
}

func newMockConn(reader io.Reader, writer io.Writer) *mockConn {
	return &mockConn{
		reader:     reader,
		writer:     writer,
		localAddr:  &mockAddr{},
		remoteAddr: &mockAddr{},
	}
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.reader.Read(p)
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.writer.Write(p)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return m.localAddr }
func (m *mockConn) RemoteAddr() net.Addr               { return m.remoteAddr }
func (m *mockConn) SetDeadline(_ time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(_ time.Time) error { return nil }

// errorWriter is a writer that always returns an error.
type errorWriter struct {
	err error
}

func (ew *errorWriter) Write(_ []byte) (n int, err error) {
	return 0, ew.err // Always return the configured error
}

// delayedReader is a reader that delays before returning data
type delayedReader struct {
	reader  io.Reader
	delay   time.Duration
	readErr error // Optional error to return after delay
}

func (dr *delayedReader) Read(p []byte) (n int, err error) {
	time.Sleep(dr.delay)
	if dr.readErr != nil {
		return 0, dr.readErr
	}
	return dr.reader.Read(p) //nolint:wrapcheck
}

// TestServerServe tests the Server.Serve method
func TestServerServe(t *testing.T) {
	// Create a valid ApiVersions request (API key 18, version 4)
	apiVersionsRequest := []byte{
		0x00, 0x00, 0x00, 0x08, // Size (8 bytes)
		0x00, 0x12, // ApiKey = 18 (ApiVersions)
		0x00, 0x04, // ApiVersion = 4
		0x00, 0x00, 0x00, 0x01, // CorrelationID = 1
	}

	tests := []struct {
		name        string
		setupServer func() (*server.Server, *mockListener)
		wantErr     bool
	}{
		{
			name: "successful connection handling",
			setupServer: func() (*server.Server, *mockListener) {
				// Create a mock connection with the request data
				reqBuf := bytes.NewBuffer(apiVersionsRequest)
				respBuf := &bytes.Buffer{}
				conn := newMockConn(reqBuf, respBuf)

				// Create a mock listener that will return our mock connection
				listener := &mockListener{
					connections: []net.Conn{conn},
					addr:        &mockAddr{},
				}

				// Create a server with our mock listener
				srv := server.NewServer(listener)

				return srv, listener
			},
			wantErr: false,
		},
		{
			name: "listener accept error",
			setupServer: func() (*server.Server, *mockListener) {
				// Create a mock listener that will return an error on Accept
				listener := &mockListener{
					acceptErr: errors.New("accept error"),
					addr:      &mockAddr{},
				}

				// Create a server with our mock listener
				srv := server.NewServer(listener)

				return srv, listener
			},
			wantErr: false, // The server should continue listening even after an accept error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, listener := tt.setupServer()

			serveErrChan := make(chan error, 1) // Buffered channel to avoid blocking

			// Run Serve in a goroutine
			go func() {
				serveErrChan <- srv.Serve()
				close(serveErrChan) // Close channel when Serve returns
			}()

			// Allow some time for the server to potentially start accepting
			// or encounter an immediate error before we close the listener.
			time.Sleep(5 * time.Millisecond)

			// Close the listener from the main test goroutine to signal Serve to stop
			if err := listener.Close(); err != nil {
				// Log the error but don't fail the test here,
				// as the main goal is to check the Serve() error.
				t.Logf("Error closing listener during test: %v", err)
			}

			// Wait for Serve to return and get its error
			err := <-serveErrChan

			// Check if the error matches expectations
			if (err != nil) != tt.wantErr {
				// Check specifically for net.ErrClosed which is expected when closing the listener
				if !(errors.Is(err, net.ErrClosed) && !tt.wantErr) {
					t.Errorf("Server.Serve() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

// --- Tests moved from handlers_test.go ---

// TestHandleConnection tests the server's internal handleConnection method.
func TestHandleConnection(t *testing.T) {
	// Helper to create test input data (size prefix + payload)
	createInput := func(payload []byte) []byte {
		// #nosec G115 -- Conversion is safe in this context
		size := uint32(len(payload))
		input := make([]byte, 4+size)
		binary.BigEndian.PutUint32(input[0:4], size)
		copy(input[4:], payload)
		return input
	}

	// Create a minimal server instance (listener is not used by handleConnection directly)
	srv := server.NewServer(nil) // Pass nil listener

	testCases := []struct {
		name           string
		inputData      []byte
		expectedOutput []byte
		writer         io.Writer // Allows injecting different writers (e.g., errorWriter)
		expectWriteErr bool      // Whether the WriteResponse call is expected to fail
		expectEOF      bool      // Whether the read operation should result in EOF
	}{
		{
			name: "Valid Request",
			inputData: createInput([]byte{
				0x00, 0x12, // ApiKey = 18
				0x00, 0x04, // ApiVersion = 4
				0x12, 0x34, 0x56, 0x78, // CorrelationID = 305419896
			}),
			expectedOutput: []byte{
				// Size = 26 (Header 4 + Body 22)
				// Header = CorrelationID (4)
				// Body = ErrorCode(2) + ArrayLenVarint(1) + ApiKeyEntry1(7)
				//      + ApiKeyEntry2(7) + ThrottleTime(4) + TaggedFields(1) = 22
				0x00, 0x00, 0x00, 0x1a, // Size = 26
				0x12, 0x34, 0x56, 0x78, // CorrelationID = 305419896
				0x00, 0x00, // ErrorCode = 0 (Success)
				0x03, // ApiKeys Array Length = 2+1 = 3 (UVarint)
				// API Key 18 (ApiVersions)
				0x00, 0x12, // ApiKey = 18
				0x00, 0x00, // MinVersion = 0
				0x00, 0x04, // MaxVersion = 4
				0x00, // Tagged Fields (ApiKey Entry)
				// API Key 75 (DescribeTopicPartitions)
				0x00, 0x4b, // ApiKey = 75
				0x00, 0x00, // MinVersion = 0
				0x00, 0x00, // MaxVersion = 0
				0x00, // Tagged Fields (ApiKey Entry)
				// End of Array
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (Overall Response)
			},
			writer:         &bytes.Buffer{}, // Use a standard buffer for output capture
			expectWriteErr: false,
			expectEOF:      false, // Expect normal processing, not EOF
		},
		{
			name:           "EOF on Read",
			inputData:      []byte{}, // Empty input causes EOF immediately
			expectedOutput: []byte{}, // No output expected
			writer:         &bytes.Buffer{},
			expectWriteErr: false, // WriteResponse won't be called
			expectEOF:      true,
		},
		{
			name: "Parse Error (Incomplete Header)",
			inputData: []byte{
				0x00, 0x00, 0x00, 0x0C, // Size = 12
				0x00, 0x12, // Only 2 bytes of payload (incomplete header)
			},
			expectedOutput: []byte{}, // No output expected
			writer:         &bytes.Buffer{},
			expectWriteErr: false, // WriteResponse won't be called
			expectEOF:      false, // Should get ErrUnexpectedEOF from protocol.ParseRequest, not EOF directly
		},
		{
			name: "Write Error",
			inputData: createInput([]byte{
				0x00, 0x12, // ApiKey = 18
				0x00, 0x04, // ApiVersion = 4
				0xAA, 0xBB, 0xCC, 0xDD, // CorrelationID
			}),
			expectedOutput: []byte{}, // No output expected as write fails
			// Use a static error for simulated write errors
			writer: &errorWriter{err: errors.New("simulated write error")}, //nolint:err113

			expectWriteErr: true, // protocol.WriteResponse should return an error
			expectEOF:      false,
		},
		{
			name: "Unsupported ApiVersion",
			inputData: createInput([]byte{
				0x00, 0x12, // ApiKey = 18
				0x00, 0x03, // ApiVersion = 3 (Unsupported)
				0x87, 0x65, 0x43, 0x21, // CorrelationID = 2271560481
			}),
			expectedOutput: []byte{
				// Size = 12 (Header 4 + Body 8)
				// Header = CorrelationID (4)
				// Body = ErrorCode(2) + ArrayLenVarint(1) + ThrottleTime(4) + TaggedFields(1) = 8
				0x00, 0x00, 0x00, 0x0c, // Size = 12
				0x87, 0x65, 0x43, 0x21, // CorrelationID = 2271560481
				0x00, 0x23, // ErrorCode = 35 (UNSUPPORTED_VERSION)
				0x01,                   // ApiKeys Array Length = 0+1 = 1 (UVarint)
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (Overall Response)
			},
			writer:         &bytes.Buffer{},
			expectWriteErr: false,
			expectEOF:      false,
		},
		// Add a test case for an unsupported API key
		{
			name: "Unsupported ApiKey",
			inputData: createInput([]byte{
				0x00, 0x01, // ApiKey = 1 (Fetch) - Assuming unsupported for now
				0x00, 0x04, // ApiVersion = 4
				0xDE, 0xAD, 0xBE, 0xEF, // CorrelationID
			}),
			// Expect an UNSUPPORTED_VERSION error response
			expectedOutput: []byte{
				0x00, 0x00, 0x00, 0x0c, // Size = 12
				0xDE, 0xAD, 0xBE, 0xEF, // CorrelationID
				0x00, 0x23, // ErrorCode = 35 (UNSUPPORTED_VERSION)
				0x01,                   // ApiKeys Array Length = 0+1 = 1 (UVarint)
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (Overall Response)
			},
			writer:         &bytes.Buffer{},
			expectWriteErr: false,
			expectEOF:      false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inputReader := bytes.NewReader(tc.inputData)
			outputWriter := tc.writer // Use the writer specified in the test case

			conn := newMockConn(inputReader, outputWriter) // Use local newMockConn

			// Call the unexported function under test
			srv.HandleConnection(conn, server.ConnectionReadTimeout) // Use server constant

			// Assertions
			if buf, ok := outputWriter.(*bytes.Buffer); ok && !tc.expectWriteErr {
				// Check output only if no write error was expected and the writer is a buffer
				// We compare output even in EOF/ParseError cases if expectedOutput is defined
				if !bytes.Equal(buf.Bytes(), tc.expectedOutput) {
					t.Errorf("Output mismatch:\nExpected: %x\nGot:      %x", tc.expectedOutput, buf.Bytes())
				}
			} else if !ok && !tc.expectWriteErr {
				// If the writer wasn't a buffer, but we didn't expect a write error, something is wrong
				t.Errorf("Expected a bytes.Buffer writer, but got %T", outputWriter)
			}
			// Note: We don't check for specific errors returned by handleConnection itself,
			// as it only logs them. We infer success/failure based on output and connection state.

			// Check if the connection was closed
			if !conn.closed {
				t.Errorf("Expected connection to be closed, but it wasn't")
			}
		})
	}
}

// TestHandleConnectionMultipleRequests tests that handleConnection can process multiple
// requests on the same connection before closing.
//
//nolint:cyclop,funlen // let the AI write a long verbose test
func TestHandleConnectionMultipleRequests(t *testing.T) {
	// Create a minimal server instance
	srv := server.NewServer(nil)

	// Create a valid ApiVersions request
	createAPIVersionsRequest := func(correlationID uint32) []byte {
		payload := []byte{
			0x00, 0x12, // ApiKey = 18 (ApiVersions)
			0x00, 0x04, // ApiVersion = 4
			0x00, 0x00, 0x00, 0x00, // CorrelationID placeholder
		}
		// Set the correlation ID
		binary.BigEndian.PutUint32(payload[4:8], correlationID)

		// Add size prefix
		size := uint32(len(payload)) //nolint:gosec // in this test, we know the payload size is small
		request := make([]byte, 4+size)
		binary.BigEndian.PutUint32(request[0:4], size)
		copy(request[4:], payload)
		return request
	}

	// Create multiple requests with different correlation IDs
	request1 := createAPIVersionsRequest(0x11111111)
	request2 := createAPIVersionsRequest(0x22222222)
	request3 := createAPIVersionsRequest(0x33333333)

	// Combine all requests into a single input stream
	combinedRequests := append(append(request1, request2...), request3...)

	// Create a reader that will return the combined requests and then EOF
	inputReader := bytes.NewReader(combinedRequests)
	outputWriter := &bytes.Buffer{}

	conn := newMockConn(inputReader, outputWriter) // Use local newMockConn

	// Call the function under test
	srv.HandleConnection(conn, server.ConnectionReadTimeout) // Use server constant

	// Verify the connection was closed
	if !conn.closed {
		t.Errorf("Expected connection to be closed, but it wasn't")
	}

	// Check that we got 3 responses by looking for the 3 correlation IDs in the output
	output := outputWriter.Bytes()

	// Debug output
	t.Logf("Output buffer length: %d bytes", len(output))

	// Each response should have the same correlation ID as its request
	expectedIDs := []uint32{0x11111111, 0x22222222, 0x33333333}
	foundIDs := make([]uint32, 0, 3)

	// The response format has the correlation ID at bytes 4-7 of each message
	// First, we need to parse the output into individual responses
	var pos int

	for pos < len(output) {
		// Each response starts with a 4-byte size field
		if pos+4 > len(output) {
			t.Logf("Incomplete response at position %d", pos)
			break
		}

		size := binary.BigEndian.Uint32(output[pos : pos+4])
		t.Logf("Found response with size %d at position %d", size, pos)

		if pos+4+int(size) > len(output) {
			t.Logf("Response size %d exceeds buffer at position %d", size, pos)
			break
		}

		// Check correlation ID (at offset 4 in the response)
		if pos+8 <= len(output) {
			correlationID := binary.BigEndian.Uint32(output[pos+4 : pos+8])
			t.Logf("Found correlation ID: 0x%08x", correlationID)
			foundIDs = append(foundIDs, correlationID)
		}

		// Move to the next response
		pos += 4 + int(size)
	}

	// Check if we found all expected correlation IDs
	if len(foundIDs) != len(expectedIDs) {
		t.Errorf("Expected %d responses, found %d", len(expectedIDs), len(foundIDs))
	}

	// Check if all expected IDs were found
	missingIDs := make([]uint32, 0)
	for _, expectedID := range expectedIDs {
		found := false
		for _, foundID := range foundIDs {
			if foundID == expectedID {
				found = true
				break
			}
		}
		if !found {
			missingIDs = append(missingIDs, expectedID)
		}
	}

	if len(missingIDs) > 0 {
		t.Errorf("Missing responses for correlation IDs: %v", missingIDs)
	}
}

// TestHandleConnectionTimeout tests that handleConnection properly times out
// when no data is received within the timeout period.
func TestHandleConnectionTimeout(t *testing.T) {
	// Create a minimal server instance
	srv := server.NewServer(nil)

	// Set a very small timeout for the test
	testTimeout := 10 * time.Millisecond

	// Create a reader that delays longer than the timeout
	delayedReader := &delayedReader{
		reader: bytes.NewReader([]byte{}), // Empty reader
		delay:  testTimeout * 2,           // Delay twice as long as the timeout
	}

	outputWriter := &bytes.Buffer{}

	conn := newMockConn(delayedReader, outputWriter) // Use local newMockConn

	// Start timing the operation
	start := time.Now()

	// Call the function under test, passing the specific test timeout
	srv.HandleConnection(conn, testTimeout)

	// Check that the operation completed in approximately the timeout duration
	elapsed := time.Since(start)

	// The connection should be closed
	if !conn.closed {
		t.Errorf("Expected connection to be closed after timeout, but it wasn't")
	}

	// The elapsed time should be close to the timeout
	// Allow some wiggle room for processing overhead
	if elapsed < testTimeout {
		t.Errorf("Operation completed too quickly: %v (expected at least %v)", elapsed, testTimeout)
	}

	// The buffer should be empty since no response should have been sent
	if outputWriter.Len() > 0 {
		t.Errorf("Expected empty output buffer after timeout, got %d bytes", outputWriter.Len())
	}
}
