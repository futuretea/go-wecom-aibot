package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

func TestSessionRequestReceivesCorrelatedResponse(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.start(ctx)
	defer session.stop(context.Canceled)

	request, err := protocol.EncodePing("request-1")
	if err != nil {
		t.Fatalf("EncodePing() error = %v", err)
	}
	result := make(chan error, 1)
	go func() {
		err := session.request(ctx, "request-1", request)
		result <- err
	}()

	select {
	case <-conn.writes:
	case <-time.After(time.Second):
		t.Fatal("session did not write request")
	}
	conn.reads <- fakeRead{
		data: []byte(`{"headers":{"req_id":"request-1"},"errcode":0,"errmsg":"ok"}`),
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("request() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not receive correlated response")
	}
}

func TestSessionRequestTimesOutAndRemovesPending(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, 20*time.Millisecond, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.start(ctx)
	defer session.stop(context.Canceled)

	request, err := protocol.EncodePing("timeout-request")
	if err != nil {
		t.Fatalf("EncodePing() error = %v", err)
	}
	err = session.request(ctx, "timeout-request", request)
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("request() error = %v, want ErrRequestTimeout", err)
	}

	session.mu.Lock()
	_, exists := session.pending["timeout-request"]
	session.mu.Unlock()
	if exists {
		t.Fatal("timed out request remains pending")
	}
}

func TestSessionHeartbeatSendsPingAndAcceptsResponse(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, 100*time.Millisecond, 20*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.start(ctx)
	session.startHeartbeat()
	defer session.stop(context.Canceled)

	var request map[string]any
	select {
	case data := <-conn.writes:
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("session did not send heartbeat")
	}
	if request["cmd"] != "ping" {
		t.Fatalf("heartbeat cmd = %v, want ping", request["cmd"])
	}
	headers := request["headers"].(map[string]any)
	requestID := headers["req_id"].(string)
	conn.reads <- fakeRead{
		data: []byte(`{"headers":{"req_id":"` + requestID + `"},"errcode":0,"errmsg":"ok"}`),
	}
}

func TestSessionRejectsConcurrentRequestID(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.start(ctx)
	defer session.stop(context.Canceled)

	request, _ := protocol.EncodePing("same-request")
	first := make(chan error, 1)
	go func() {
		err := session.request(ctx, "same-request", request)
		first <- err
	}()
	<-conn.writes

	err := session.request(ctx, "same-request", request)
	if !errors.Is(err, ErrRequestInFlight) {
		t.Fatalf("second request error = %v, want ErrRequestInFlight", err)
	}
	session.stop(context.Canceled)
	<-first
}

func TestSessionUnexpectedResponseStopsWithProtocolError(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.start(ctx)

	conn.reads <- fakeRead{
		data: []byte(`{"headers":{"req_id":"unknown"},"errcode":0,"errmsg":"ok"}`),
	}
	err := session.wait()
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("wait() error = %v, want ProtocolError", err)
	}
}

func TestSessionHeartbeatTimeoutStopsWithConnectionError(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, 20*time.Millisecond, 10*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.start(ctx)
	session.startHeartbeat()

	err := session.wait()
	var connectionErr *ConnectionError
	if !errors.As(err, &connectionErr) {
		t.Fatalf("wait() error = %v, want ConnectionError", err)
	}
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("wait() error = %v, want ErrRequestTimeout cause", err)
	}
}

func TestSessionParentCancellationStopsWait(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	session.start(ctx)

	cancel()
	result := make(chan error, 1)
	go func() {
		result <- session.wait()
	}()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait() error = %v, want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wait() did not return after parent context cancellation")
	}
	select {
	case <-conn.closed:
	default:
		t.Fatal("connection remains open after parent context cancellation")
	}
}

type fakeRead struct {
	data []byte
	err  error
}

type fakeConnection struct {
	reads  chan fakeRead
	writes chan []byte
	closed chan struct{}
}

func newFakeConnection() *fakeConnection {
	return &fakeConnection{
		reads:  make(chan fakeRead, 32),
		writes: make(chan []byte, 32),
		closed: make(chan struct{}),
	}
}

func (c *fakeConnection) Read(ctx context.Context) ([]byte, error) {
	select {
	case read := <-c.reads:
		return read.data, read.err
	case <-c.closed:
		return nil, errors.New("closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConnection) Write(ctx context.Context, data []byte) error {
	select {
	case c.writes <- append([]byte(nil), data...):
		return nil
	case <-c.closed:
		return errors.New("closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fakeConnection) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}
