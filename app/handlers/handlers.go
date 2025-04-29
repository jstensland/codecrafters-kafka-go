// Package handlers contains the logic for handling client connections and requests specific to Kafka API keys.
package handlers

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// BaseRequest represents the structure of an incoming Kafka request header
type BaseRequest struct {
	Size           uint32
	APIKey         uint16
	APIVersion     uint16
	CorrelationID  uint32
	RemainingBytes []byte // The rest of the payload after the header fields
}

// ParseRequest reads from the reader and parses the Kafka request header
func ParseRequest(reader io.Reader) (*BaseRequest, error) {
	// 1. Read the message size (first 4 bytes)
	sizeBytes := make([]byte, protocol.SizeFieldLength)
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
		return nil, fmt.Errorf("%w: %d", protocol.ErrMessageSizeExceedsLimit, messageSize)
	}
	// Minimum header size: APIKey(2) + APIVersion(2) + CorrelationID(4) = 8
	minHeaderSize := uint32(APIKeyFieldLength + VersionFieldLength + protocol.CorrelationIDLength)
	if messageSize < minHeaderSize {
		return nil, fmt.Errorf("%w: %d", protocol.ErrMessageSizeTooSmall, messageSize)
	}

	// 2. Read the rest of the message payload (messageSize bytes)
	// The payload contains APIKey, APIVersion, CorrelationID, etc.
	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("reading message payload: %w", err)
	}

	// 3. Parse fields from the payload
	apiKeyEnd := APIKeyFieldLength
	versionEnd := apiKeyEnd + VersionFieldLength
	correlationEnd := versionEnd + protocol.CorrelationIDLength

	req := &BaseRequest{
		Size:           messageSize,
		APIKey:         binary.BigEndian.Uint16(payload[0:apiKeyEnd]),
		APIVersion:     binary.BigEndian.Uint16(payload[apiKeyEnd:versionEnd]),
		CorrelationID:  binary.BigEndian.Uint32(payload[versionEnd:correlationEnd]),
		RemainingBytes: payload[correlationEnd:], // Store the rest if needed later
	}

	return req, nil
}

// String provides a human-readable representation of the BaseRequest, including hex values.
func (r *BaseRequest) String() string {
	// Reconstruct the header bytes for hex representation
	headerBytes := make([]byte, protocol.SizeFieldLength+APIKeyFieldLength+VersionFieldLength+protocol.CorrelationIDLength)
	binary.BigEndian.PutUint32(headerBytes[0:4], r.Size)
	binary.BigEndian.PutUint16(headerBytes[4:6], r.APIKey)
	binary.BigEndian.PutUint16(headerBytes[6:8], r.APIVersion)
	binary.BigEndian.PutUint32(headerBytes[8:12], r.CorrelationID)

	return fmt.Sprintf(
		"BaseRequest{\n"+
			"  Size:          %d (0x%X)\n"+
			"  APIKey:        %d (0x%X)\n"+
			"  APIVersion:    %d (0x%X)\n"+
			"  CorrelationID: %d (0x%X)\n"+
			"  Header Hex:    %X\n"+
			"  Payload Hex:   %X\n"+
			"}",
		r.Size, r.Size,
		r.APIKey, r.APIKey,
		r.APIVersion, r.APIVersion,
		r.CorrelationID, r.CorrelationID,
		headerBytes,
		r.RemainingBytes,
	)
}
