package wecomaibot

import (
	"context"
	"sync"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

// Handler processes supported inbound messages.
type Handler interface {
	HandleMessage(context.Context, *Message) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *Message) error

// HandleMessage calls f(ctx, message).
func (f HandlerFunc) HandleMessage(ctx context.Context, message *Message) error {
	return f(ctx, message)
}

type handlerDispatcher struct {
	handler   Handler
	sessionID string
	semaphore chan struct{}

	wg    sync.WaitGroup
	mu    sync.Mutex
	cause error
}

func newHandlerDispatcher(handler Handler, sessionID string) *handlerDispatcher {
	return &handlerDispatcher{
		handler:   handler,
		sessionID: sessionID,
		semaphore: make(chan struct{}, handlerLimit),
	}
}

func (d *handlerDispatcher) dispatch(ctx context.Context, current *session, frame protocol.Frame) error {
	message, err := messageFromCallback(d.sessionID, frame.RequestID, frame.Callback)
	if err != nil {
		return &ProtocolError{Err: err}
	}
	select {
	case d.semaphore <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrHandlerOverload
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer func() { <-d.semaphore }()
		if err := d.handler.HandleMessage(ctx, message); err != nil {
			handlerErr := &HandlerError{Err: err}
			d.mu.Lock()
			if d.cause == nil {
				d.cause = handlerErr
			}
			d.mu.Unlock()
			current.stop(handlerErr)
		}
	}()
	return nil
}

func (d *handlerDispatcher) wait() {
	d.wg.Wait()
}

func (d *handlerDispatcher) err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cause
}
