package wecomaibot

import (
	"fmt"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

// ChatType identifies a single or group conversation.
type ChatType string

const (
	// ChatTypeSingle is a direct conversation with one user.
	ChatTypeSingle ChatType = "single"
	// ChatTypeGroup is a group conversation.
	ChatTypeGroup ChatType = "group"
)

// MessageType identifies the supported inbound message type.
type MessageType string

const (
	// MessageTypeText is an inbound text message.
	MessageTypeText MessageType = "text"
)

// User identifies the sender of a message.
type User struct {
	ID string
}

// Quote contains supported quoted message content.
type Quote struct {
	Type MessageType
	Text string
}

// Message is a supported inbound WeCom message.
type Message struct {
	ID       string
	BotID    string
	ChatID   string
	ChatType ChatType
	From     User
	Type     MessageType
	Text     string
	Quote    *Quote

	requestID string
	sessionID string
}

// TextContent returns the text body, or an empty string for non-text messages.
func (m *Message) TextContent() string {
	if m == nil || m.Type != MessageTypeText {
		return ""
	}
	return m.Text
}

func messageFromCallback(
	sessionID string,
	requestID string,
	callback protocol.Callback,
) (*Message, error) {
	if sessionID == "" || requestID == "" || callback.MessageID == "" || callback.UserID == "" {
		return nil, fmt.Errorf("invalid text callback")
	}

	chatType := ChatType(callback.ChatType)
	chatID := callback.ChatID
	switch chatType {
	case ChatTypeSingle:
		chatID = callback.UserID
	case ChatTypeGroup:
		if chatID == "" {
			return nil, fmt.Errorf("invalid group callback: missing chat id")
		}
	default:
		return nil, fmt.Errorf("invalid text callback: unknown chat type %q", callback.ChatType)
	}

	message := &Message{
		ID:        callback.MessageID,
		BotID:     callback.BotID,
		ChatID:    chatID,
		ChatType:  chatType,
		From:      User{ID: callback.UserID},
		Type:      MessageTypeText,
		Text:      callback.Text,
		requestID: requestID,
		sessionID: sessionID,
	}
	if callback.Quote != nil {
		message.Quote = &Quote{
			Type: MessageType(callback.Quote.Type),
			Text: callback.Quote.Text,
		}
	}
	return message, nil
}
