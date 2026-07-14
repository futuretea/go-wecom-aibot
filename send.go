package wecomaibot

import (
	"context"
	"fmt"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

// StreamUpdate is one update to a caller-owned stream reply.
type StreamUpdate struct {
	ID      string
	Content string
	Finish  bool
}

// Target identifies a proactive message destination.
type Target struct {
	ID       string
	ChatType ChatType
}

// ReplyMarkdown replies to message with Markdown content.
func (c *Client) ReplyMarkdown(ctx context.Context, message *Message, content string) error {
	active, err := c.replySession(ctx, message, content)
	if err != nil {
		return err
	}
	data, err := protocol.EncodeReplyMarkdown(message.requestID, content)
	if err != nil {
		return &ProtocolError{Err: fmt.Errorf("encode markdown reply: %w", err)}
	}
	err = active.session.request(ctx, message.requestID, data)
	return err
}

// ReplyStream sends one caller-owned stream update in reply to message.
func (c *Client) ReplyStream(ctx context.Context, message *Message, update StreamUpdate) error {
	active, err := c.replySession(ctx, message, update.Content)
	if err != nil {
		return err
	}
	if update.ID == "" {
		return fmt.Errorf("%w: stream ID is required", ErrInvalidArgument)
	}
	data, err := protocol.EncodeReplyStream(
		message.requestID,
		update.ID,
		update.Content,
		update.Finish,
	)
	if err != nil {
		return &ProtocolError{Err: fmt.Errorf("encode stream reply: %w", err)}
	}
	err = active.session.request(ctx, message.requestID, data)
	return err
}

// SendMarkdown proactively sends Markdown content to target.
func (c *Client) SendMarkdown(ctx context.Context, target Target, content string) error {
	if ctx == nil || target.ID == "" || content == "" {
		return fmt.Errorf("%w: context, target ID, and content are required", ErrInvalidArgument)
	}
	var wireChatType uint32
	switch target.ChatType {
	case ChatTypeSingle:
		wireChatType = 1
	case ChatTypeGroup:
		wireChatType = 2
	default:
		return fmt.Errorf("%w: invalid chat type %q", ErrInvalidArgument, target.ChatType)
	}
	active := c.currentSession()
	if active == nil {
		return ErrNotConnected
	}
	requestID := newRequestID()
	data, err := protocol.EncodeSendMarkdown(requestID, target.ID, wireChatType, content)
	if err != nil {
		return &ProtocolError{Err: fmt.Errorf("encode markdown send: %w", err)}
	}
	err = active.session.request(ctx, requestID, data)
	return err
}

func (c *Client) replySession(
	ctx context.Context,
	message *Message,
	content string,
) (*activeSession, error) {
	if ctx == nil || message == nil || content == "" {
		return nil, fmt.Errorf("%w: context, message, and content are required", ErrInvalidArgument)
	}
	active := c.currentSession()
	if active == nil {
		return nil, ErrNotConnected
	}
	if message.sessionID != active.id {
		return nil, ErrStaleMessage
	}
	return active, nil
}

func (c *Client) currentSession() *activeSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	active := c.active
	if active == nil {
		return nil
	}
	select {
	case <-active.session.done:
		return nil
	default:
		return active
	}
}
