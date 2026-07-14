package wecomaibot

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

type frameHandler func(context.Context, protocol.Frame) error

type session struct {
	conn           connection
	requestTimeout time.Duration
	heartbeat      time.Duration
	handle         frameHandler

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	stopOnce  sync.Once
	writeGate chan struct{}
	mu        sync.Mutex
	cause     error
	pending   map[string]chan protocol.Frame
	wg        sync.WaitGroup
}

func newSession(
	conn connection,
	requestTimeout time.Duration,
	heartbeat time.Duration,
	handle frameHandler,
) *session {
	session := &session{
		conn:           conn,
		requestTimeout: requestTimeout,
		heartbeat:      heartbeat,
		handle:         handle,
		done:           make(chan struct{}),
		writeGate:      make(chan struct{}, 1),
		pending:        make(map[string]chan protocol.Frame),
	}
	session.writeGate <- struct{}{}
	return session
}

func (s *session) start(parent context.Context) {
	s.ctx, s.cancel = context.WithCancel(parent)
	s.wg.Add(2)
	go s.readLoop()
	go func() {
		defer s.wg.Done()
		select {
		case <-parent.Done():
			s.stop(parent.Err())
		case <-s.done:
		}
	}()
}

func (s *session) startHeartbeat() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.sendHeartbeat(); err != nil {
					if s.ctx.Err() == nil {
						s.stop(err)
					}
					return
				}
			case <-s.done:
				return
			}
		}
	}()
}

func (s *session) sendHeartbeat() error {
	requestID, err := newRequestID()
	if err != nil {
		return &ProtocolError{Err: fmt.Errorf("generate heartbeat request id: %w", err)}
	}
	data, err := protocol.EncodePing(requestID)
	if err != nil {
		return &ProtocolError{Err: fmt.Errorf("encode heartbeat: %w", err)}
	}
	_, err = s.request(s.ctx, requestID, data)
	if errors.Is(err, ErrRequestTimeout) {
		return &ConnectionError{Err: fmt.Errorf("heartbeat: %w", err)}
	}
	return err
}

func (s *session) readLoop() {
	defer s.wg.Done()
	for {
		data, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				var messageTypeErr *unexpectedWebSocketMessageTypeError
				if errors.As(err, &messageTypeErr) {
					s.stop(&ProtocolError{Err: err})
				} else {
					s.stop(&ConnectionError{Err: err})
				}
			}
			return
		}
		frame, err := protocol.Decode(data)
		if err != nil {
			s.stop(&ProtocolError{Err: err})
			return
		}
		if err := s.route(frame); err != nil {
			s.stop(err)
			return
		}
	}
}

func (s *session) route(frame protocol.Frame) error {
	switch frame.Kind {
	case protocol.KindResponse:
		s.mu.Lock()
		response, ok := s.pending[frame.RequestID]
		if ok {
			delete(s.pending, frame.RequestID)
		}
		s.mu.Unlock()
		if !ok {
			return &ProtocolError{Err: fmt.Errorf("unexpected response for request %q", frame.RequestID)}
		}
		response <- frame
		return nil
	case protocol.KindIgnored:
		return nil
	case protocol.KindDisconnected:
		return ErrConnectionReplaced
	case protocol.KindTextCallback:
		if s.handle == nil {
			return nil
		}
		return s.handle(s.ctx, frame)
	default:
		return &ProtocolError{Err: fmt.Errorf("unexpected frame kind %q", frame.Kind)}
	}
}

func (s *session) request(
	ctx context.Context,
	requestID string,
	data []byte,
) (protocol.Frame, error) {
	requestCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	response, err := s.registerPending(requestID)
	if err != nil {
		return protocol.Frame{}, err
	}
	if err := s.sendPendingRequest(ctx, requestCtx, requestID, data, response); err != nil {
		return protocol.Frame{}, err
	}
	return s.awaitResponse(ctx, requestCtx, requestID, response)
}

func (s *session) registerPending(requestID string) (chan protocol.Frame, error) {
	response := make(chan protocol.Frame, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pending[requestID]; exists {
		return nil, ErrRequestInFlight
	}
	s.pending[requestID] = response
	return response, nil
}

func (s *session) sendPendingRequest(
	ctx context.Context,
	requestCtx context.Context,
	requestID string,
	data []byte,
	response chan protocol.Frame,
) error {
	writeStarted, err := s.write(requestCtx, data)
	if err == nil {
		return nil
	}
	s.removePending(requestID, response)
	return s.classifyWriteError(ctx, requestCtx, writeStarted, err)
}

func (s *session) classifyWriteError(
	ctx context.Context,
	requestCtx context.Context,
	writeStarted bool,
	err error,
) error {
	if !writeStarted && ctx.Err() != nil {
		return ctx.Err()
	}
	if requestCtx.Err() != nil && ctx.Err() == nil {
		connectionErr := &ConnectionError{Err: fmt.Errorf("write request: %w", ErrRequestTimeout)}
		s.stop(connectionErr)
		return ErrRequestTimeout
	}
	select {
	case <-s.done:
		return s.err()
	default:
	}
	connectionErr := &ConnectionError{Err: err}
	s.stop(connectionErr)
	return s.err()
}

func (s *session) awaitResponse(
	ctx context.Context,
	requestCtx context.Context,
	requestID string,
	response chan protocol.Frame,
) (protocol.Frame, error) {
	select {
	case frame := <-response:
		if frame.ErrorCode != 0 {
			return protocol.Frame{}, &APIError{
				Code:    frame.ErrorCode,
				Message: frame.ErrorMessage,
			}
		}
		return frame, nil
	case <-requestCtx.Done():
		s.removePending(requestID, response)
		if ctx.Err() != nil {
			return protocol.Frame{}, ctx.Err()
		}
		return protocol.Frame{}, ErrRequestTimeout
	case <-s.done:
		s.removePending(requestID, response)
		return protocol.Frame{}, s.err()
	}
}

func (s *session) write(ctx context.Context, data []byte) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.done:
		return false, s.err()
	case <-s.writeGate:
	}
	defer func() {
		s.writeGate <- struct{}{}
	}()
	return true, s.conn.Write(ctx, data)
}

func (s *session) removePending(requestID string, response chan protocol.Frame) {
	s.mu.Lock()
	if s.pending[requestID] == response {
		delete(s.pending, requestID)
	}
	s.mu.Unlock()
}

func (s *session) stop(cause error) {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.cause = cause
		s.mu.Unlock()
		close(s.done)
		if s.cancel != nil {
			s.cancel()
		}
		_ = s.conn.Close()
	})
}

func (s *session) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cause != nil {
		return s.cause
	}
	return context.Canceled
}

func (s *session) wait() error {
	<-s.done
	s.wg.Wait()
	return s.err()
}

func newRequestID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}
