package wecomaibot

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRunReconnectsAndResetsBackoffAfterSubscription(t *testing.T) {
	firstConn := newFakeConnection()
	secondConn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{
		connections: []connection{nil, nil, firstConn, secondConn},
		errors:      []error{errors.New("dial one"), errors.New("dial two"), nil, nil},
	}

	retryDelays := make(chan time.Duration, 3)
	reconnectGap := make(chan struct{})
	releaseReconnect := make(chan struct{})
	client.config.OnRetry = func(_ error, delay time.Duration) { retryDelays <- delay }
	client.waitRetry = func(ctx context.Context, _ time.Duration) error {
		if len(retryDelays) == 3 {
			close(reconnectGap)
			select {
			case <-releaseReconnect:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages := make(chan *Message, 2)
	runResult := make(chan error, 1)
	go func() {
		runResult <- client.Run(ctx, HandlerFunc(func(_ context.Context, message *Message) error {
			messages <- message
			return nil
		}))
	}()

	respondOK(firstConn, requestID(t, readRequest(t, firstConn)))
	firstConn.reads <- fakeRead{data: textCallback("old-request", "old-message")}
	oldMessage := <-messages
	firstConn.reads <- fakeRead{err: errors.New("read failed")}
	<-reconnectGap
	assertReplyError(t, client, oldMessage, "gap", ErrNotConnected)
	close(releaseReconnect)

	respondOK(secondConn, requestID(t, readRequest(t, secondConn)))
	secondConn.reads <- fakeRead{data: textCallback("new-request", "new-message")}
	<-messages
	assertReplyError(t, client, oldMessage, "stale", ErrStaleMessage)

	assertRetryDelays(t, retryDelays, []time.Duration{
		2 * time.Second,
		5 * time.Second,
		2 * time.Second,
	})
	cancel()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func assertReplyError(t *testing.T, client *Client, message *Message, content string, want error) {
	t.Helper()
	if err := client.ReplyMarkdown(context.Background(), message, content); !errors.Is(err, want) {
		t.Fatalf("ReplyMarkdown() error = %v, want %v", err, want)
	}
}

func assertRetryDelays(t *testing.T, delays <-chan time.Duration, want []time.Duration) {
	t.Helper()
	got := make([]time.Duration, len(want))
	for index := range got {
		got[index] = <-delays
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("retry delays = %v, want %v", got, want)
	}
}

func TestDisconnectFailsPendingSendAndCleansCorrelation(t *testing.T) {
	client, conn, _, cancel, runResult := startClientWithMessage(t)
	client.waitRetry = func(ctx context.Context, _ time.Duration) error {
		<-ctx.Done()
		return ctx.Err()
	}
	active := client.currentSession()

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- client.SendMarkdown(
			context.Background(),
			Target{ID: "user", ChatType: ChatTypeSingle},
			"pending",
		)
	}()
	request := readRequest(t, conn)
	requestID := requestID(t, request)
	conn.reads <- fakeRead{err: errors.New("connection lost")}

	select {
	case err := <-sendResult:
		var connectionErr *ConnectionError
		if !errors.As(err, &connectionErr) {
			t.Fatalf("SendMarkdown() error = %v, want ConnectionError", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending send did not fail after disconnect")
	}
	active.session.mu.Lock()
	_, exists := active.session.pending[requestID]
	active.session.mu.Unlock()
	if exists {
		t.Fatal("disconnected request remains pending")
	}
	cancel()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestRunErrorPriority(t *testing.T) {
	t.Run("handler over connection", func(t *testing.T) {
		err := runWithConcurrentStops(t, false)
		var handlerErr *HandlerError
		if !errors.As(err, &handlerErr) || handlerErr.Err.Error() != "handler failed" {
			t.Fatalf("Run() error = %v, want HandlerError", err)
		}
	})

	t.Run("parent context over handler", func(t *testing.T) {
		err := runWithConcurrentStops(t, true)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	})
}

func runWithConcurrentStops(t *testing.T, cancelParent bool) error {
	t.Helper()
	conn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{conn}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- client.Run(ctx, HandlerFunc(func(context.Context, *Message) error {
			close(started)
			<-release
			return errors.New("handler failed")
		}))
	}()
	respondOK(conn, requestID(t, readRequest(t, conn)))
	conn.reads <- fakeRead{data: textCallback("priority-request", "priority-message")}
	<-started
	if cancelParent {
		cancel()
	} else {
		conn.reads <- fakeRead{err: errors.New("connection failed")}
	}
	close(release)
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("Run() did not resolve concurrent stop causes")
		return nil
	}
}
