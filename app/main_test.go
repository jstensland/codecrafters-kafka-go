package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

// mockAddr satisfies the net.Addr interface for testing.
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

// --- TestParseRequest remains unchanged ---

func TestParseRequest(t *testing.T) {
	// Helper to create test input data (size prefix + payload)
	createInput := func(payload []byte) []byte {
		size := uint32(len(payload))
		input := make([]byte, 4+size)
		binary.BigEndian.PutUint32(input[0:4], size)
		copy(input[4:], payload)
		return input
	}

	// Test cases
	testCases := []struct {
		name         string
		inputData    []byte
		expectedReq  *Request
		expectedErr  error  // Use errors.Is for checking specific error types like io.EOF
		expectErrStr string // Use for checking specific error messages
	}{
		{
			name: "Valid Request with Remaining Bytes",
			inputData: createInput([]byte{
				0x00, 0x12, // ApiKey = 18
				0x00, 0x04, // ApiVersion = 4
				0x6f, 0x7f, 0xc6, 0x61, // CorrelationID = 1870644833
				0xDE, 0xAD, 0xBE, 0xEF, // RemainingBytes
			}),
			expectedReq: &Request{
				Size:           12,
				ApiKey:         18,
				ApiVersion:     4,
				CorrelationID:  1870644833,
				RemainingBytes: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
			expectedErr: nil,
		},
		{
			name: "Valid Request Exact Header Size",
			inputData: createInput([]byte{
				0x00, 0x12, // ApiKey = 18
				0x00, 0x04, // ApiVersion = 4
				0x6f, 0x7f, 0xc6, 0x61, // CorrelationID = 1870644833
			}),
			expectedReq: &Request{
				Size:           8,
				ApiKey:         18,
				ApiVersion:     4,
				CorrelationID:  1870644833,
				RemainingBytes: []byte{},
			},
			expectedErr: nil,
		},
		{
			name:        "Error Unexpected EOF Reading Size", // Renamed for clarity
			inputData:   []byte{0x00, 0x00},                  // Incomplete size
			expectedReq: nil,
			expectedErr: io.ErrUnexpectedEOF, // ReadFull returns ErrUnexpectedEOF here, which gets wrapped
		},
		{
			name: "Error EOF Reading Payload",
			inputData: []byte{
				0x00, 0x00, 0x00, 0x0C, // Size = 12
				0x00, 0x12, 0x00, 0x04, // Only 4 bytes of payload provided
			},
			expectedReq: nil,
			expectedErr: io.ErrUnexpectedEOF, // ReadFull returns this when fewer bytes are read
		},
		{
			name: "Error Message Size Too Small",
			inputData: createInput([]byte{
				0x00, 0x12, // ApiKey
				0x00, 0x04, // ApiVersion
			}), // Payload size is 4, which is < 8
			expectedReq:  nil,
			expectedErr:  nil, // Remove placeholder, rely on expectErrStr
			expectErrStr: "message size 4 is too small for header",
		},
		{
			name:        "Error Empty Input",
			inputData:   []byte{},
			expectedReq: nil,
			expectedErr: io.EOF,
		},
		// Add more cases if needed, e.g., message size exceeds limit (if implemented strictly)
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader(tc.inputData)
			req, err := ParseRequest(reader)

			// Check for specific error types
			if tc.expectedErr != nil {
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("Expected error type %v, got %v", tc.expectedErr, err)
				}
			}

			// Check for specific error messages if provided
			if tc.expectErrStr != "" {
				if err == nil || err.Error() != tc.expectErrStr {
					t.Errorf("Expected error message '%s', got '%v'", tc.expectErrStr, err)
				}
			} else if tc.expectedErr == nil && err != nil {
				// If no error string or type was expected, but we got an error
				t.Errorf("Expected no error, got %v", err)
			}

			// Check if an error occurred when none was expected (and no specific type/string was checked)
			if tc.expectedErr == nil && tc.expectErrStr == "" && err != nil {
				t.Errorf("Expected no error, but got: %v", err)
			}

			// Check if no error occurred when one was expected
			if (tc.expectedErr != nil || tc.expectErrStr != "") && err == nil {
				t.Errorf("Expected an error (%v / '%s'), but got nil", tc.expectedErr, tc.expectErrStr)
			}

			// Compare the request struct if no error was expected
			if tc.expectedErr == nil && tc.expectErrStr == "" {
				if !reflect.DeepEqual(req, tc.expectedReq) {
					t.Errorf("Request mismatch:\nExpected: %+v\nGot:      %+v", tc.expectedReq, req)
				}
			}
		})
	}
}

func TestHandleConnection(t *testing.T) {
	// Helper to create test input data (size prefix + payload)
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
				// Size = 21 (Header 4 + Body 17)
				// Header = CorrelationID (4)
				// Body = ErrorCode(2) + ArrayLen(4) + ApiKeyEntry(6) + ThrottleTime(4) + TaggedFields(1) = 17
				0x00, 0x00, 0x00, 0x15, // Size = 21
				0x12, 0x34, 0x56, 0x78, // CorrelationID = 305419896
				0x00, 0x00, // ErrorCode = 0 (Success)
				0x00, 0x00, 0x00, 0x01, // ApiKeys Array Length = 1
				0x00, 0x12, // ApiKey = 18 (ApiVersions)
				0x00, 0x00, // MinVersion = 0 (Updated in main.go)
				0x00, 0x04, // MaxVersion = 4 (Updated in main.go)
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (empty)
			},
			writer:         &bytes.Buffer{}, // Use a standard buffer for output capture
			expectWriteErr: false,
			expectEOF:      false,
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
				0x00, 0x12, // Only 2 bytes of payload
			},
			expectedOutput: []byte{}, // No output expected
			writer:         &bytes.Buffer{},
			expectWriteErr: false, // WriteResponse won't be called
			expectEOF:      false, // Should get ErrUnexpectedEOF from ParseRequest, not EOF directly
		},
		{
			name: "Write Error",
			inputData: createInput([]byte{
				0x00, 0x12, 0x00, 0x04, 0xAA, 0xBB, 0xCC, 0xDD, // Valid request
			}),
			expectedOutput: []byte{}, // No output expected as write fails
			writer:         &errorWriter{err: errors.New("simulated write error")},
			expectWriteErr: true, // WriteResponse should return an error
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
				// Size = 15 (Header 4 + Body 11)
				// Header = CorrelationID (4)
				// Body = ErrorCode(2) + ArrayLen(4) + ThrottleTime(4) + TaggedFields(1) = 11
				0x00, 0x00, 0x00, 0x0f, // Size = 15
				0x87, 0x65, 0x43, 0x21, // CorrelationID = 2271560481
				0x00, 0x23, // ErrorCode = 35 (UNSUPPORTED_VERSION)
				0x00, 0x00, 0x00, 0x00, // ApiKeys Array Length = 0
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (empty)
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

			// If the test case expects a normal write, outputWriter will be *bytes.Buffer
			// If it expects a write error, it will be *errorWriter
			conn := &mockConn{
				reader: inputReader,
				writer: outputWriter,
			}

			// Call the function under test
			HandleConnection(conn)

			// Assertions
			// Check output buffer content if a successful write was expected
			if buf, ok := outputWriter.(*bytes.Buffer); ok && !tc.expectWriteErr && !tc.expectEOF && len(tc.inputData) >= 12 { // Only check output if input was valid enough to potentially generate output
				if !bytes.Equal(buf.Bytes(), tc.expectedOutput) {
					t.Errorf("Output mismatch:\nExpected: %x\nGot:      %x", tc.expectedOutput, buf.Bytes())
				}
			}
			// Note: We don't explicitly check for the *absence* of output in error cases (EOF, parse error, write error)
			// because HandleConnection only logs errors, it doesn't return them. The primary check is that it runs without panic
			// and closes the connection. The `expectWriteErr` flag helps distinguish cases where the error originates
			// during the write phase vs. the read/parse phase.

			// Check if the connection was closed
			if !conn.closed {
				t.Errorf("Expected connection to be closed, but it wasn't")
			}
		})
	}
}

func TestWriteResponse(t *testing.T) {
	testCases := []struct {
		name           string
		response       *Response
		expectedOutput []byte
		writer         io.Writer // Use bytes.Buffer or errorWriter
		expectedErr    error
	}{
		{
			name: "Successful Response - Single API Key",
			response: &Response{
				CorrelationID: 12345,
				ErrorCode:     0,
				ApiKeys: []ApiKeyVersion{
					{ApiKey: 18, MinVersion: 4, MaxVersion: 4},
				},
				ThrottleTimeMs: 0,
			},
			expectedOutput: []byte{
				// Size = 21 (Header 4 + Body 17)
				0x00, 0x00, 0x00, 0x15, // Size = 21
				0x00, 0x00, 0x30, 0x39, // CorrelationID = 12345
				0x00, 0x00, // ErrorCode = 0
				0x00, 0x00, 0x00, 0x01, // ApiKeys Array Length = 1
				0x00, 0x12, // ApiKey = 18
				0x00, 0x04, // MinVersion = 4
				0x00, 0x04, // MaxVersion = 4
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (empty)
			},
			writer:      &bytes.Buffer{},
			expectedErr: nil,
		},
		{
			name: "Successful response with multiple API keys",
			response: &Response{
				CorrelationID: 54321,
				ErrorCode:     0,
				ApiKeys: []ApiKeyVersion{
					{ApiKey: 0, MinVersion: 1, MaxVersion: 9},  // Produce
					{ApiKey: 1, MinVersion: 1, MaxVersion: 13}, // Fetch
					{ApiKey: 18, MinVersion: 0, MaxVersion: 4}, // ApiVersions
				},
				ThrottleTimeMs: 100, // Example throttle time
			},
			expectedOutput: []byte{
				// Size = 33 (Header 4 + Body 29)
				// Body = Err(2) + Len(4) + Key1(6) + Key2(6) + Key3(6) + Throttle(4) + TaggedFields(1) = 29
				0x00, 0x00, 0x00, 0x21, // Size = 33
				0x00, 0x00, 0xD4, 0x31, // CorrelationID = 54321
				0x00, 0x00, // ErrorCode = 0
				0x00, 0x00, 0x00, 0x03, // ApiKeys Array Length = 3
				0x00, 0x00, 0x00, 0x01, 0x00, 0x09, // Produce v1-9
				0x00, 0x01, 0x00, 0x01, 0x00, 0x0D, // Fetch v1-13
				0x00, 0x12, 0x00, 0x00, 0x00, 0x04, // ApiVersions v0-4
				0x00, 0x00, 0x00, 0x64, // ThrottleTimeMs = 100
				0x00, // Tagged Fields (empty)
			},
			writer:      &bytes.Buffer{},
			expectedErr: nil,
		},
		{
			name: "Error Response - Unsupported Version",
			response: &Response{
				CorrelationID:  9876,
				ErrorCode:      35,                // UNSUPPORTED_VERSION
				ApiKeys:        []ApiKeyVersion{}, // Must be empty
				ThrottleTimeMs: 0,
			},
			expectedOutput: []byte{
				// Size = 15 (Header 4 + Body 11)
				// Body = Err(2) + Len(4) + Throttle(4) + TaggedFields(1) = 11
				0x00, 0x00, 0x00, 0x0f, // Size = 15
				0x00, 0x00, 0x26, 0x94, // CorrelationID = 9876
				0x00, 0x23, // ErrorCode = 35
				0x00, 0x00, 0x00, 0x00, // ApiKeys Array Length = 0
				0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs = 0
				0x00, // Tagged Fields (empty)
			},
			writer:      &bytes.Buffer{},
			expectedErr: nil,
		},
		{
			name: "Write Error",
			response: &Response{ // Content doesn't matter much here
				CorrelationID:  111,
				ErrorCode:      0,
				ApiKeys:        []ApiKeyVersion{{ApiKey: 18, MinVersion: 4, MaxVersion: 4}},
				ThrottleTimeMs: 0,
			},
			expectedOutput: []byte{}, // No output expected
			writer:         &errorWriter{err: errors.New("failed to write")},
			expectedErr:    errors.New("writing response: failed to write"), // Expect wrapped error
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := WriteResponse(tc.writer, tc.response)

			// Check error
			if tc.expectedErr != nil {
				if err == nil || err.Error() != tc.expectedErr.Error() {
					t.Errorf("Expected error '%v', got '%v'", tc.expectedErr, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error, got '%v'", err)
			}

			// Check output if using bytes.Buffer and no error expected
			if buf, ok := tc.writer.(*bytes.Buffer); ok && tc.expectedErr == nil {
				if !bytes.Equal(buf.Bytes(), tc.expectedOutput) {
					t.Errorf("Output mismatch:\nExpected: %x\nGot:      %x", tc.expectedOutput, buf.Bytes())
				}
			}
		})
	}
}
