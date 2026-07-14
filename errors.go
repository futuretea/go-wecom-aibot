package wecomaibot

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	// ErrInvalidConfig reports invalid client configuration.
	ErrInvalidConfig = errors.New("wecomaibot: invalid config")
	// ErrNotConnected reports that no subscribed session is active.
	ErrNotConnected = errors.New("wecomaibot: not connected")
	// ErrRequestInFlight reports concurrent use of one protocol request ID.
	ErrRequestInFlight = errors.New("wecomaibot: request already in flight")
	// ErrRequestTimeout reports that WeCom did not respond in time.
	ErrRequestTimeout = errors.New("wecomaibot: request timeout")
	// ErrConnectionReplaced reports that another connection replaced this client.
	ErrConnectionReplaced = errors.New("wecomaibot: connection replaced")
	// ErrHandlerOverload reports that the handler concurrency limit was reached.
	ErrHandlerOverload = errors.New("wecomaibot: handler concurrency limit reached")
	// ErrAlreadyRunning reports concurrent calls to Client.Run.
	ErrAlreadyRunning = errors.New("wecomaibot: client is already running")
	// ErrStaleMessage reports a reply attempted with a message from an old session.
	ErrStaleMessage = errors.New("wecomaibot: message belongs to a stale session")
	// ErrInvalidArgument reports invalid input to a public operation.
	ErrInvalidArgument = errors.New("wecomaibot: invalid argument")
)

// APIError is an error response returned by WeCom.
type APIError struct {
	Code    int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("wecomaibot: API error %d: %s", e.Code, strconv.Quote(e.Message))
}

// ProtocolError reports a malformed or unexpected WeCom frame.
type ProtocolError struct {
	Err error
}

func (e *ProtocolError) Error() string {
	return "wecomaibot: protocol error: " + e.Err.Error()
}

func (e *ProtocolError) Unwrap() error {
	return e.Err
}

// ConnectionError reports a WebSocket transport failure.
type ConnectionError struct {
	Err error
}

func (e *ConnectionError) Error() string {
	return "wecomaibot: connection error: " + strconv.Quote(e.Err.Error())
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// HandlerError wraps an error returned by a message handler.
type HandlerError struct {
	Err error
}

func (e *HandlerError) Error() string {
	return "wecomaibot: handler error: " + e.Err.Error()
}

func (e *HandlerError) Unwrap() error {
	return e.Err
}
