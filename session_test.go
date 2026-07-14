package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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

func TestSessionRequestTimeoutBoundsBlockedWrite(t *testing.T) {
	conn := newBlockingWriteConnection()
	session := newSession(conn, 20*time.Millisecond, time.Hour, nil)
	session.start(context.Background())

	request, _ := protocol.EncodePing("blocked-write")
	result := make(chan error, 1)
	go func() {
		err := session.request(context.Background(), "blocked-write", request)
		result <- err
	}()

	select {
	case err := <-result:
		if !errors.Is(err, ErrRequestTimeout) {
			t.Fatalf("request() error = %v, want ErrRequestTimeout", err)
		}
	case <-time.After(100 * time.Millisecond):
		session.stop(context.Canceled)
		<-result
		t.Fatal("request() did not time out while Write was blocked")
	}

	select {
	case <-session.done:
	case <-time.After(100 * time.Millisecond):
		session.stop(context.Canceled)
		t.Fatal("session remained active after a blocked Write timed out")
	}
}

func TestSessionRequestTimeoutBoundsQueuedWrite(t *testing.T) {
	conn := newBlockingWriteConnection()
	session := newSession(conn, 30*time.Millisecond, time.Hour, nil)
	session.start(context.Background())

	firstRequest, _ := protocol.EncodePing("first-write")
	first := make(chan error, 1)
	go func() {
		err := session.request(context.Background(), "first-write", firstRequest)
		first <- err
	}()
	<-conn.writeStarted

	secondRequest, _ := protocol.EncodePing("queued-write")
	second := make(chan error, 1)
	go func() {
		err := session.request(context.Background(), "queued-write", secondRequest)
		second <- err
	}()

	deadline := time.After(150 * time.Millisecond)
	for completed := 0; completed < 2; completed++ {
		select {
		case <-first:
			first = nil
		case <-second:
			second = nil
		case <-deadline:
			session.stop(context.Canceled)
			if first != nil {
				<-first
			}
			if second != nil {
				<-second
			}
			t.Fatal("queued writes did not complete within the request timeout")
		}
	}
}

func TestSessionRequestCancellationBeforeWriteKeepsSessionActive(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	session.start(context.Background())
	defer session.stop(context.Canceled)

	<-session.writeGate
	defer func() { session.writeGate <- struct{}{} }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request, _ := protocol.EncodePing("canceled-before-write")

	err := session.request(ctx, "canceled-before-write", request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("request() error = %v, want context.Canceled", err)
	}
	select {
	case <-session.done:
		t.Fatal("session stopped before a network write began")
	default:
	}
	select {
	case data := <-conn.writes:
		t.Fatalf("unexpected network write: %s", data)
	default:
	}
}

func TestSessionRequestPreCanceledContextWithAvailableWriteGateDoesNotWrite(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	session.start(context.Background())
	defer session.stop(context.Canceled)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	const attempts = 256
	for i := 0; i < attempts; i++ {
		requestID := "pre-canceled-" + strconv.Itoa(i)
		request, err := protocol.EncodePing(requestID)
		if err != nil {
			t.Fatalf("EncodePing() error = %v", err)
		}

		err = session.request(ctx, requestID, request)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request() attempt %d error = %v, want context.Canceled", i, err)
		}
	}

	select {
	case data := <-conn.writes:
		t.Fatalf("unexpected network write: %s", data)
	default:
	}
	session.mu.Lock()
	pending := len(session.pending)
	session.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending request count = %d, want 0", pending)
	}
	select {
	case <-session.done:
		t.Fatal("session stopped for a request canceled before writing")
	default:
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

type blockingWriteConnection struct {
	*fakeConnection
	writeStarted chan struct{}
}

func newBlockingWriteConnection() *blockingWriteConnection {
	return &blockingWriteConnection{
		fakeConnection: newFakeConnection(),
		writeStarted:   make(chan struct{}, 2),
	}
}

func (c *blockingWriteConnection) Write(ctx context.Context, _ []byte) error {
	c.writeStarted <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("closed")
	}
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
