package wecomaibot

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionStopPublishesProtocolErrorBeforeClosingConnection(t *testing.T) {
	conn := &blockingCloseConnection{
		closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
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
	closeStarted chan struct{}
	releaseClose chan struct{}
}

func (c *blockingCloseConnection) Read(context.Context) ([]byte, error) {
	return nil, errors.New("unexpected read")
}

func (c *blockingCloseConnection) Write(context.Context, []byte) error {
	return errors.New("unexpected write")
}

func (c *blockingCloseConnection) Close() error {
	close(c.closeStarted)
	<-c.releaseClose
	return nil
}
