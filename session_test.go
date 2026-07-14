package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"sync"
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

func TestSessionSendPendingRequestCanceledAfterWriteGateAcquisitionDoesNotWrite(t *testing.T) {
	conn := newFakeConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	defer session.stop(context.Canceled)

	const requestID = "post-acquire-canceled"
	response, err := session.registerPending(requestID)
	if err != nil {
		t.Fatalf("registerPending() error = %v", err)
	}
	requestCtx := &postAcquireCancelContext{
		Context:    context.Background(),
		errChecked: make(chan struct{}),
		canceled:   make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() {
		result <- session.sendPendingRequest(
			requestCtx, requestCtx, requestID, []byte(requestID), response,
		)
	}()

	select {
	case <-requestCtx.errChecked:
		requestCtx.cancel()
	case data := <-conn.writes:
		requestCtx.cancel()
		t.Fatalf("connection.Write() received %q before post-acquire cancellation check", data)
	case <-time.After(time.Second):
		requestCtx.cancel()
		t.Fatal("sendPendingRequest() did not reach the post-acquire context check")
	}

	err = receiveSessionResult(t, result)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sendPendingRequest() error = %v, want context.Canceled", err)
	}
	select {
	case data := <-conn.writes:
		t.Fatalf("unexpected network write after cancellation: %q", data)
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
		t.Fatal("session stopped for a request canceled before transport write")
	default:
	}
}

func TestSessionWriteFailurePublishesBeforeQueuedWriteStarts(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	writeErr := errors.New("first write failed")
	conn := newWriteFailureHandoffConnection(writeErr)
	session := newSession(conn, time.Second, time.Hour, nil)

	firstResponse, err := session.registerPending("first-write")
	if err != nil {
		t.Fatalf("registerPending(first-write) error = %v", err)
	}
	firstResult, _ := sendPendingRequestAsync(session, "first-write", firstResponse)
	<-conn.firstWriteStarted

	secondResponse, err := session.registerPending("second-write")
	if err != nil {
		t.Fatalf("registerPending(second-write) error = %v", err)
	}
	secondResult, secondSendStarted := sendPendingRequestAsync(session, "second-write", secondResponse)
	<-secondSendStarted
	runtime.Gosched()

	session.mu.Lock()
	close(conn.releaseFirstWrite)
	runtime.Gosched()
	writeCalls := conn.writeCalls()
	session.mu.Unlock()

	firstErr := receiveSessionResult(t, firstResult)
	secondErr := receiveSessionResult(t, secondResult)
	if firstErr == nil || !errors.Is(firstErr, writeErr) {
		t.Fatalf("first request error = %v, want first write failure", firstErr)
	}
	if secondErr != firstErr {
		t.Fatalf("second request error = %v, want original terminal cause %v", secondErr, firstErr)
	}
	if writeCalls != 1 {
		t.Fatalf("connection Write calls before terminal publication = %d, want 1", writeCalls)
	}
	session.mu.Lock()
	pending := len(session.pending)
	session.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending request count = %d, want 0", pending)
	}
}

func receiveSessionResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("session request did not complete")
		return nil
	}
}

func sendPendingRequestAsync(
	session *session,
	requestID string,
	response chan protocol.Frame,
) (<-chan error, <-chan struct{}) {
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		result <- session.sendPendingRequest(
			context.Background(), context.Background(), requestID, []byte(requestID), response,
		)
	}()
	return result, started
}

type postAcquireCancelContext struct {
	context.Context
	errChecked chan struct{}
	canceled   chan struct{}
	errOnce    sync.Once
}

func (c *postAcquireCancelContext) Done() <-chan struct{} {
	return c.canceled
}

func (c *postAcquireCancelContext) Err() error {
	c.errOnce.Do(func() {
		close(c.errChecked)
		<-c.canceled
	})
	return context.Canceled
}

func (c *postAcquireCancelContext) cancel() {
	close(c.canceled)
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

type writeFailureHandoffConnection struct {
	*fakeConnection
	firstWriteStarted chan struct{}
	releaseFirstWrite chan struct{}
	writeErr          error

	mu    sync.Mutex
	calls int
}

func newBlockingWriteConnection() *blockingWriteConnection {
	return &blockingWriteConnection{
		fakeConnection: newFakeConnection(),
		writeStarted:   make(chan struct{}, 2),
	}
}

func newWriteFailureHandoffConnection(writeErr error) *writeFailureHandoffConnection {
	return &writeFailureHandoffConnection{
		fakeConnection:    newFakeConnection(),
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
		writeErr:          writeErr,
	}
}

func (c *writeFailureHandoffConnection) Write(context.Context, []byte) error {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()

	if call == 1 {
		close(c.firstWriteStarted)
		<-c.releaseFirstWrite
		return c.writeErr
	}
	return nil
}

func (c *writeFailureHandoffConnection) writeCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
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
