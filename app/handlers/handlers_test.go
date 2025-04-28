package handlers_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

// errorWriter is a writer that always returns an error.
type errorWriter struct {
	err error
}

// Type aliases for testing
type Request = handlers.BaseRequest

func (ew *errorWriter) Write(_ []byte) (n int, err error) {
	return 0, ew.err // Always return the configured error
}

func TestParseRequest(t *testing.T) { //nolint:gocognit,cyclop // Test function with many test cases
	// Helper to create test input data (size prefix + payload)
	createInput := func(payload []byte) []byte {
		// #nosec G115 -- Conversion is safe in this context
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
				APIKey:         18,
				APIVersion:     4,
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
				APIKey:         18,
				APIVersion:     4,
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
			expectErrStr: "message size too small for header: 4",
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
			req, err := handlers.ParseRequest(reader)

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
