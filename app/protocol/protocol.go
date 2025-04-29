// Package protocol implements the Kafka wire protocol for request and response handling.
package protocol

import (
	"errors"
	"fmt"
	"io"
)

// Bit mask for varint encoding
const varintContinueBit = 0x80

// Error types for protocol operations
var (
	ErrMessageSizeExceedsLimit = errors.New("message size exceeds limit")
	ErrMessageSizeTooSmall     = errors.New("message size too small for header")
	ErrSizeMismatch            = errors.New("calculated response size does not match actual written size")
)

// Serializable defines an interface for types that can be serialized into a byte slice.
type Serializable interface {
	Serialize() []byte
}

// WriteUvarint encodes a uint64 as an unsigned varint into the provided byte slice
// and returns the number of bytes written.
// It panics if the buffer is too small.
func WriteUvarint(buf []byte, x uint64) int {
	i := 0
	for x >= varintContinueBit {
		buf[i] = byte(x) | varintContinueBit
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

// Kafka protocol constants
const (
	// Field sizes in bytes
	SizeFieldLength     = 4
	CorrelationIDLength = 4
	ErrorCodeLength     = 2

	// Error codes (Exported)
	ErrorNone                    = 0
	ErrorUnknownTopicOrPartition = 3
	ErrorUnsupportedVersion      = 35
)

// WriteCompactArray encodes a slice of Serializable items into the Kafka COMPACT_ARRAY format
// into the provided buffer. It returns the number of bytes written.
// Assumes the buffer `buf` is large enough.
func WriteCompactArray(buf []byte, items []Serializable) int {
	offset := 0

	// Write Array Length (N+1) as Uvarint
	// #nosec G115 -- Conversion is safe in this context
	nBytes := WriteUvarint(buf[offset:], uint64(len(items)+1))
	offset += nBytes

	// Write Array Elements
	for _, item := range items {
		serialized := item.Serialize()
		itemLen := len(serialized)
		copy(buf[offset:offset+itemLen], serialized)
		offset += itemLen
	}
	return offset
}

// APIResponse defines the interface for any Kafka response type that can be serialized.
type APIResponse interface {
	Serialize() ([]byte, error)
}

// WriteResponse takes any response implementing APIResponse, serializes it,
// and writes it to the provided writer.
func WriteResponse(writer io.Writer, resp APIResponse) error {
	responseBytes, err := resp.Serialize()
	if err != nil {
		return fmt.Errorf("serializing response: %w", err)
	}

	// Send the complete response
	_, err = writer.Write(responseBytes)
	if err != nil {
		return fmt.Errorf("writing response bytes: %w", err)
	}
	return nil
}
