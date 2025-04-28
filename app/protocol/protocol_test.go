package protocol_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/stretchr/testify/assert"
)

// mockSerializable implements the protocol.Serializable interface for testing.
type mockSerializable struct {
	data []byte
}

func (m *mockSerializable) Serialize() []byte {
	return m.data
}

// mockAPIResponse implements the protocol.APIResponse interface for testing.
type mockAPIResponse struct {
	data []byte
	err  error
}

func (m *mockAPIResponse) Serialize() ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.data, nil
}

// errorWriter implements io.Writer but always returns an error.
type errorWriter struct {
	err error
}

func (ew *errorWriter) Write(_ []byte) (n int, err error) {
	return 0, ew.err
}

func TestWriteUvarint(t *testing.T) {
	testCases := []struct {
		name        string
		value       uint64
		expectedBuf []byte
		expectedLen int
	}{
		{"zero", 0, []byte{0x00}, 1},
		{"one", 1, []byte{0x01}, 1},
		{"127", 127, []byte{0x7f}, 1},
		{"128", 128, []byte{0x80, 0x01}, 2},
		{"16383", 16383, []byte{0xff, 0x7f}, 2},
		{"16384", 16384, []byte{0x80, 0x80, 0x01}, 3},
		{"large value", 2097151, []byte{0xff, 0xff, 0x7f}, 3},
		{"larger value", 268435455, []byte{0xff, 0xff, 0xff, 0x7f}, 4},
		{"max uint64", 18446744073709551615, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 10) // Max varint length for uint64
			n := protocol.WriteUvarint(buf, tc.value)

			if n != tc.expectedLen {
				t.Errorf("WriteUvarint(%d): expected length %d, got %d", tc.value, tc.expectedLen, n)
			}
			if !bytes.Equal(buf[:n], tc.expectedBuf) {
				t.Errorf("WriteUvarint(%d): expected buffer %x, got %x", tc.value, tc.expectedBuf, buf[:n])
			}
		})
	}
}

func TestWriteCompactArray(t *testing.T) {
	testCases := []struct {
		name        string
		items       []protocol.Serializable
		expectedBuf []byte
		expectedLen int
	}{
		{
			name:        "empty array",
			items:       []protocol.Serializable{},
			expectedBuf: []byte{0x01}, // Length 0+1 = 1
			expectedLen: 1,
		},
		{
			name: "single item",
			items: []protocol.Serializable{
				&mockSerializable{data: []byte{0xaa, 0xbb}},
			},
			expectedBuf: []byte{0x02, 0xaa, 0xbb}, // Length 1+1 = 2, item data
			expectedLen: 3,
		},
		{
			name: "multiple items",
			items: []protocol.Serializable{
				&mockSerializable{data: []byte{0x01}},
				&mockSerializable{data: []byte{0x02, 0x03}},
				&mockSerializable{data: []byte{0x04, 0x05, 0x06}},
			},
			// Length 3+1 = 4, item1, item2, item3
			expectedBuf: []byte{0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06},
			expectedLen: 7,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Estimate buffer size: max varint len + size of all items
			maxBufSize := 10
			for _, item := range tc.items {
				maxBufSize += len(item.Serialize())
			}
			buf := make([]byte, maxBufSize)

			n := protocol.WriteCompactArray(buf, tc.items)

			if n != tc.expectedLen {
				t.Errorf("WriteCompactArray: expected length %d, got %d", tc.expectedLen, n)
			}
			if !bytes.Equal(buf[:n], tc.expectedBuf) {
				t.Errorf("WriteCompactArray: expected buffer %x, got %x", tc.expectedBuf, buf[:n])
			}
		})
	}
}

func TestWriteResponse(t *testing.T) {
	testCases := []struct {
		name          string
		response      protocol.APIResponse
		writer        *bytes.Buffer // Use buffer to check output
		errorWriter   *errorWriter  // Use error writer for write errors
		expectedBytes []byte
		expectedErr   error
	}{
		{
			name: "successful write",
			response: &mockAPIResponse{
				data: []byte{0x00, 0x00, 0x00, 0x05, 0xaa, 0xbb, 0xcc, 0xdd, 0xee}, // Includes size prefix
			},
			writer:        &bytes.Buffer{},
			expectedBytes: []byte{0x00, 0x00, 0x00, 0x05, 0xaa, 0xbb, 0xcc, 0xdd, 0xee},
			expectedErr:   nil,
		},
		{
			name: "serialization error",
			response: &mockAPIResponse{
				err: assert.AnError,
			},
			writer:        &bytes.Buffer{},
			expectedBytes: []byte{}, // No bytes should be written
			expectedErr:   assert.AnError,
		},
		{
			name: "writer error",
			response: &mockAPIResponse{
				data: []byte{0x00, 0x00, 0x00, 0x01, 0xff}, // Includes size prefix
			},
			errorWriter: &errorWriter{err: assert.AnError},
			expectedErr: assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			switch {
			case tc.writer != nil:
				err = protocol.WriteResponse(tc.writer, tc.response)
			case tc.errorWriter != nil:
				err = protocol.WriteResponse(tc.errorWriter, tc.response)
			default:
				t.Fatal("Test case must provide either writer or errorWriter")
			}

			if !errors.Is(err, tc.expectedErr) {
				t.Errorf("WriteResponse: expected error %v, got %v", tc.expectedErr, err)
			}

			if tc.writer != nil && !bytes.Equal(tc.writer.Bytes(), tc.expectedBytes) {
				t.Errorf("WriteResponse: expected bytes %x, got %x", tc.expectedBytes, tc.writer.Bytes())
			}
		})
	}
}
