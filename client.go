package wecomaibot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

const (
	requestTimeout  = 15 * time.Second
	heartbeatPeriod = 30 * time.Second
	handlerLimit    = 16
)

var retrySchedule = [...]time.Duration{
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

// Client manages one active WeCom AI Bot connection.
type Client struct {
	config    Config
	connector connector
	waitRetry func(context.Context, time.Duration) error

	mu      sync.RWMutex
	running bool
	active  *activeSession
}

type activeSession struct {
	id      string
	session *session
}

// NewClient validates config and constructs a client without opening a connection.
func NewClient(config Config) (*Client, error) {
	if config.BotID == "" {
		return nil, fmt.Errorf("%w: BotID is required", ErrInvalidConfig)
	}
	if config.Secret == "" {
		return nil, fmt.Errorf("%w: Secret is required", ErrInvalidConfig)
	}
	return &Client{
		config:    config,
		connector: websocketConnector{endpoint: defaultEndpoint},
		waitRetry: waitForRetry,
	}, nil
}

// Run connects the bot and handles messages until ctx is canceled or a
// non-retryable error occurs.
func (c *Client) Run(ctx context.Context, handler Handler) error {
	if handlerFunc, ok := handler.(HandlerFunc); ctx == nil || handler == nil || (ok && handlerFunc == nil) {
		return fmt.Errorf("%w: context and handler are required", ErrInvalidArgument)
	}
	if !c.beginRun() {
		return ErrAlreadyRunning
	}
	defer c.endRun()

	backoff := retryBackoff{}
	for {
		subscribed, err := c.runSession(ctx, handler)
		connectionErr, terminalErr := classifyRunError(ctx, err)
		if terminalErr != nil {
			return terminalErr
		}
		if err := c.retry(ctx, connectionErr, backoff.next(subscribed)); err != nil {
			return err
		}
	}
}

type retryBackoff struct {
	index int
}

func (b *retryBackoff) next(reset bool) time.Duration {
	if reset {
		b.index = 0
	}
	delay := retrySchedule[b.index]
	if b.index < len(retrySchedule)-1 {
		b.index++
	}
	return delay
}

func classifyRunError(ctx context.Context, err error) (*ConnectionError, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var connectionErr *ConnectionError
	if !errors.As(err, &connectionErr) {
		return nil, err
	}
	return connectionErr, nil
}

func (c *Client) retry(
	ctx context.Context,
	connectionErr *ConnectionError,
	delay time.Duration,
) error {
	if c.config.OnRetry != nil {
		c.config.OnRetry(connectionErr, delay)
	}
	if err := c.waitRetry(ctx, delay); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func (c *Client) runSession(ctx context.Context, handler Handler) (bool, error) {
	conn, err := c.connector.Dial(ctx)
	if err != nil {
		return false, &ConnectionError{Err: err}
	}

	sessionID := newRequestID()
	dispatcher := newHandlerDispatcher(handler, sessionID)
	ready := make(chan struct{})
	var current *session
	current = newSession(conn, requestTimeout, heartbeatPeriod, func(ctx context.Context, frame protocol.Frame) error {
		select {
		case <-ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		return dispatcher.dispatch(ctx, current, frame)
	})
	current.start(ctx)

	if err := c.subscribe(ctx, current); err != nil {
		current.stop(err)
		_ = current.wait()
		return false, err
	}
	c.setActive(&activeSession{id: sessionID, session: current})
	close(ready)
	current.startHeartbeat()
	err = current.wait()
	c.clearActive(current)
	dispatcher.wait()

	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if handlerErr := dispatcher.err(); handlerErr != nil {
		return true, handlerErr
	}
	return true, err
}

func (c *Client) subscribe(ctx context.Context, current *session) error {
	requestID := newRequestID()
	data, err := protocol.EncodeSubscribe(requestID, c.config.BotID, c.config.Secret)
	if err != nil {
		return &ProtocolError{Err: fmt.Errorf("encode subscribe: %w", err)}
	}
	err = current.request(ctx, requestID, data)
	return err
}

func (c *Client) beginRun() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return false
	}
	c.running = true
	return true
}

func (c *Client) endRun() {
	c.mu.Lock()
	c.running = false
	c.active = nil
	c.mu.Unlock()
}

func (c *Client) setActive(active *activeSession) {
	c.mu.Lock()
	c.active = active
	c.mu.Unlock()
}

func (c *Client) clearActive(current *session) {
	c.mu.Lock()
	if c.active != nil && c.active.session == current {
		c.active = nil
	}
	c.mu.Unlock()
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
