package main_test // Change package to main_test to avoid conflicts

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	main "github.com/codecrafters-io/kafka-starter-go/app"
)

// mockAddr satisfies the net.Addr interface for testing.
// Keep this helper in main_test as it's used by mockConn for HandleConnection tests.
type mockAddr struct{}

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return "127.0.0.1:1234" }

// mockConn implements the net.Conn interface for testing purposes.
type mockConn struct {
	reader io.Reader
	writer io.Writer
	closed bool
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
	// Simulate closing the reader if it's an io.Closer
	if closer, ok := m.reader.(io.Closer); ok {
		closer.Close()
	}
	// Simulate closing the writer if it's an io.Closer
	if closer, ok := m.writer.(io.Closer); ok {
		closer.Close()
	}
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return &mockAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return &mockAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil } // No-op for mock
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil } // No-op for mock
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil } // No-op for mock

// errorWriter is a writer that always returns an error.
type errorWriter struct {
	err error
}

func (ew *errorWriter) Write(p []byte) (n int, err error) {
	return 0, ew.err // Always return the configured error
}

// TestHandleConnection remains in main_test as it tests the main package's HandleConnection function.
func TestHandleConnection(t *testing.T) {
	// Helper to create test input data (size prefix + payload)
	// Keep this helper local to the test function.
	createInput := func(payload []byte) []byte {
		size := uint32(len(payload))
		input := make([]byte, 4+size)
		binary.BigEndian.PutUint32(input[0:4], size)
		copy(input[4:], payload)
		return input
	}

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
				// Size = 19 (Header 4 + Body 15)
				// Header = CorrelationID (4)
				// Body = ErrorCode(2) + ArrayLenVarint(1) + ApiKeyEntry(7) + ThrottleTime(4) + TaggedFields(1) = 15
				0x00, 0x00, 0x00, 0x13, // Size = 19
				0x12, 0x34, 0x56, 0x78, // CorrelationID = 305419896
				0x00, 0x00, // ErrorCode = 0 (Success)
				0x02,       // ApiKeys Array Length = 1+1 = 2 (UVarint)
				0x00, 0x12, // ApiKey = 18 (ApiVersions)
				0x00, 0x00, // MinVersion = 0
				0x00, 0x04, // MaxVersion = 4
				0x00,                   // Tagged Fields (ApiKey Entry)
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
			writer:         &errorWriter{err: errors.New("simulated write error")},
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

			conn := &mockConn{
				reader: inputReader,
				writer: outputWriter,
			}

			// Call the function under test (from the main package)
			main.HandleConnection(conn) // Use main.HandleConnection

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
			// Note: We don't check for specific errors returned by HandleConnection itself,
			// as it only logs them. We infer success/failure based on output and connection state.

			// Check if the connection was closed
			if !conn.closed {
				t.Errorf("Expected connection to be closed, but it wasn't")
			}
		})
	}
}
