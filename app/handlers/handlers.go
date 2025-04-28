// Package handlers contains the logic for handling client connections and requests specific to Kafka API keys.
package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// Constants
const (
	apiVersionsV4 = 4 // Supported API version for ApiVersions
)

// HandleAPIVersionsRequest processes an ApiVersions request and returns the appropriate response
func HandleAPIVersionsRequest(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{
		CorrelationID:  req.CorrelationID,
		ThrottleTimeMs: 0, // No throttling implemented
	}

	// Currently, only ApiVersions v4 is "supported" for a successful response.
	// Other versions will result in an UNSUPPORTED_VERSION error.
	// Note: Kafka protocol allows brokers to support multiple versions.
	// A real broker would check req.ApiVersion against its supported range.
	if req.APIVersion != apiVersionsV4 {
		resp.ErrorCode = protocol.ErrorUnsupportedVersion
		resp.APIKeys = []protocol.APIKeyVersion{} // Must be empty on error
	} else {
		resp.ErrorCode = protocol.ErrorNone // Success
		// Define the APIs supported by this broker
		// Always include ApiVersions (18) and DescribeTopicPartitions (75) for successful responses
		resp.APIKeys = []protocol.APIKeyVersion{
			// Report support for versions 0 through 4 for ApiVersions
			{APIKey: protocol.APIKeyAPIVersions, MinVersion: 0, MaxVersion: apiVersionsV4},
			// Report support for version 0 for DescribeTopicPartitions
			{APIKey: protocol.APIKeyDescribeTopicPartitions, MinVersion: 0, MaxVersion: 0},
		}
	}

	return resp
}
