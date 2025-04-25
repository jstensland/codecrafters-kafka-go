//nolint:err113,wrapcheck // dynamic and passed errors in tests are okay
package server_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

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

// mockConn implements the net.Conn interface for testing
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
