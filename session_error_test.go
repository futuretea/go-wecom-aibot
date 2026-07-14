package wecomaibot

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSessionStopPublishesProtocolErrorBeforeClosingConnection(t *testing.T) {
	conn := newBlockingCloseConnection()
	session := newSession(conn, time.Second, time.Hour, nil)
	cause := errors.New("invalid frame")
	protocolErr := &ProtocolError{Err: cause}
	stopReturned := make(chan struct{})
	go func() {
		session.stop(protocolErr)
		close(stopReturned)
	}()
	t.Cleanup(func() {
		close(conn.releaseClose)
		select {
		case <-stopReturned:
		case <-time.After(time.Second):
			t.Error("session.stop() did not return after Close was released")
		}
	})

	select {
	case <-conn.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("session.stop() did not call Close")
	}
	select {
	case <-session.done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("session.done was not closed before Close returned")
	}
	if err := session.err(); err != protocolErr {
		t.Fatalf("session.err() = %v, want original ProtocolError %v", err, protocolErr)
	}

	assertClassifyWriteErrorReturnsProtocolError(t, session, protocolErr, cause)
}

func TestSendMethodsRejectStoppedActiveSessionBeforeCloseReturns(t *testing.T) {
	conn := newBlockingCloseConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &blockingCloseConnector{conn: conn}
	runResult := make(chan error, 1)
	subscribed := make(chan struct{})
	go func() {
		runResult <- client.Run(context.Background(), HandlerFunc(func(context.Context, *Message) error {
			close(subscribed)
			return nil
		}))
	}()

	var releaseOnce sync.Once
	releaseClose := func() {
		releaseOnce.Do(func() { close(conn.releaseClose) })
	}
	t.Cleanup(releaseClose)

	respondOK(conn.fakeConnection, requestID(t, readRequest(t, conn.fakeConnection)))
	conn.reads <- fakeRead{data: textCallback("callback-stopped", "message-stopped")}
	select {
	case <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("client did not finish subscribing")
	}
	active := client.currentSession()
	if active == nil {
		t.Fatal("client has no active session after subscription")
	}
	message := &Message{requestID: "callback-stopped", sessionID: active.id}
	cause := &unexpectedWebSocketMessageTypeError{}
	conn.reads <- fakeRead{err: cause}

	select {
	case <-conn.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("session did not start closing the connection")
	}
	select {
	case <-active.session.done:
	case <-time.After(time.Second):
		t.Fatal("session did not stop before Close returned")
	}
	select {
	case err := <-runResult:
		t.Fatalf("Run() returned before Close was released: %v", err)
	default:
	}

	assertSendMethodsNotConnected(t, client, message)
	if t.Failed() {
		return
	}

	releaseClose()
	select {
	case err := <-runResult:
		if !errors.Is(err, cause) {
			t.Fatalf("Run() error = %v, want original cause %v", err, cause)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after Close was released")
	}
}

func assertSendMethodsNotConnected(t *testing.T, client *Client, message *Message) {
	t.Helper()
	errorsByMethod := map[string]error{
		"SendMarkdown": client.SendMarkdown(
			context.Background(),
			Target{ID: "user", ChatType: ChatTypeSingle},
			"send",
		),
		"ReplyMarkdown": client.ReplyMarkdown(context.Background(), message, "reply"),
		"ReplyStream": client.ReplyStream(context.Background(), message, StreamUpdate{
			ID:      "stream",
			Content: "part",
		}),
	}
	for method, err := range errorsByMethod {
		if !errors.Is(err, ErrNotConnected) {
			t.Errorf("%s() error = %v, want ErrNotConnected", method, err)
		}
	}
}

func assertClassifyWriteErrorReturnsProtocolError(
	t *testing.T,
	session *session,
	protocolErr *ProtocolError,
	cause error,
) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		result <- session.classifyWriteError(
			context.Background(),
			context.Background(),
			true,
			errors.New("write failed"),
		)
	}()
	select {
	case err := <-result:
		var gotProtocolErr *ProtocolError
		if !errors.As(err, &gotProtocolErr) || gotProtocolErr != protocolErr {
			t.Fatalf("classifyWriteError() error = %v, want original ProtocolError %v", err, protocolErr)
		}
		var connectionErr *ConnectionError
		if errors.As(err, &connectionErr) {
			t.Fatalf("classifyWriteError() error = %v, do not want ConnectionError", err)
		}
		if !errors.Is(err, cause) {
			t.Fatalf("classifyWriteError() error = %v, want original cause %v", err, cause)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("classifyWriteError() did not return while Close was blocked")
	}
}

type blockingCloseConnection struct {
	*fakeConnection
	closeStarted chan struct{}
	releaseClose chan struct{}
}

func newBlockingCloseConnection() *blockingCloseConnection {
	return &blockingCloseConnection{
		fakeConnection: newFakeConnection(),
		closeStarted:   make(chan struct{}),
		releaseClose:   make(chan struct{}),
	}
}

type blockingCloseConnector struct {
	conn connection
}

func (c *blockingCloseConnector) Dial(context.Context) (connection, error) {
	return c.conn, nil
}

func (c *blockingCloseConnection) Close() error {
	close(c.closeStarted)
	<-c.releaseClose
	return c.fakeConnection.Close()
}
