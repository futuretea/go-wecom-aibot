package protocol

import (
	"encoding/json"
	"fmt"
)

// Kind identifies a decoded protocol frame.
type Kind string

const (
	// KindTextCallback is a supported text message callback.
	KindTextCallback Kind = "text_callback"
	// KindIgnored is a known protocol callback outside the current scope.
	KindIgnored Kind = "ignored"
	// KindDisconnected reports that a newer connection replaced this one.
	KindDisconnected Kind = "disconnected"
	// KindResponse is a response to a client request.
	KindResponse Kind = "response"
)

// Frame is the internal representation of a WeCom protocol frame.
type Frame struct {
	Kind         Kind
	RequestID    string
	Callback     Callback
	ErrorCode    int
	ErrorMessage string
}

// Callback contains fields shared by supported message callbacks.
type Callback struct {
	MessageID string
	BotID     string
	ChatID    string
	ChatType  string
	UserID    string
	Text      string
	Quote     *Quote
}

// Quote contains the supported quoted text.
type Quote struct {
	Type string
	Text string
}

type envelope struct {
	Command      string          `json:"cmd"`
	Headers      headers         `json:"headers"`
	Body         json.RawMessage `json:"body"`
	ErrorCode    *int            `json:"errcode"`
	ErrorMessage string          `json:"errmsg"`
}

type headers struct {
	RequestID string `json:"req_id"`
}

type callbackBody struct {
	MessageID string `json:"msgid"`
	BotID     string `json:"aibotid"`
	ChatID    string `json:"chatid"`
	ChatType  string `json:"chattype"`
	From      user   `json:"from"`
	Type      string `json:"msgtype"`
	Text      text   `json:"text"`
	Quote     *quote `json:"quote"`
	Event     event  `json:"event"`
}

type user struct {
	ID string `json:"userid"`
}

type text struct {
	Content string `json:"content"`
}

type quote struct {
	Type string `json:"msgtype"`
	Text text   `json:"text"`
}

type event struct {
	Type string `json:"eventtype"`
}

// Decode parses one WebSocket JSON frame.
func Decode(data []byte) (Frame, error) {
	var wire envelope
	if err := json.Unmarshal(data, &wire); err != nil {
		return Frame{}, fmt.Errorf("decode frame: %w", err)
	}
	if wire.Headers.RequestID == "" {
		return Frame{}, fmt.Errorf("decode frame: missing request id")
	}
	if wire.Command == "" && wire.ErrorCode != nil {
		return Frame{
			Kind:         KindResponse,
			RequestID:    wire.Headers.RequestID,
			ErrorCode:    *wire.ErrorCode,
			ErrorMessage: wire.ErrorMessage,
		}, nil
	}
	if wire.Command != "aibot_msg_callback" && wire.Command != "aibot_event_callback" {
		return Frame{}, fmt.Errorf("decode frame: unsupported command %q", wire.Command)
	}
	return decodeCallback(wire)
}

func decodeCallback(wire envelope) (Frame, error) {
	var body callbackBody
	if err := json.Unmarshal(wire.Body, &body); err != nil {
		return Frame{}, fmt.Errorf("decode callback: %w", err)
	}
	if body.MessageID == "" {
		return Frame{}, fmt.Errorf("decode callback: missing message id")
	}
	if body.BotID == "" {
		return Frame{}, fmt.Errorf("decode callback: missing bot id")
	}
	if wire.Command == "aibot_event_callback" {
		return decodeEvent(wire.Headers.RequestID, body.Event.Type)
	}
	return decodeMessage(wire.Headers.RequestID, body)
}

func decodeEvent(requestID, eventType string) (Frame, error) {
	switch eventType {
	case "disconnected_event":
		return Frame{Kind: KindDisconnected, RequestID: requestID}, nil
	case "enter_chat", "template_card_event", "feedback_event":
		return Frame{Kind: KindIgnored, RequestID: requestID}, nil
	default:
		return Frame{}, fmt.Errorf("decode callback: unsupported event type %q", eventType)
	}
}

func decodeMessage(requestID string, body callbackBody) (Frame, error) {
	switch body.Type {
	case "image", "mixed", "voice", "file", "video":
		return Frame{Kind: KindIgnored, RequestID: requestID}, nil
	case "text":
	default:
		return Frame{}, fmt.Errorf("decode callback: unsupported message type %q", body.Type)
	}
	if err := validateTextCallback(body); err != nil {
		return Frame{}, err
	}

	callback := Callback{
		MessageID: body.MessageID,
		BotID:     body.BotID,
		ChatID:    body.ChatID,
		ChatType:  body.ChatType,
		UserID:    body.From.ID,
		Text:      body.Text.Content,
	}
	if body.Quote != nil && body.Quote.Type == "text" {
		callback.Quote = &Quote{Type: body.Quote.Type, Text: body.Quote.Text.Content}
	}

	return Frame{
		Kind:      KindTextCallback,
		RequestID: requestID,
		Callback:  callback,
	}, nil
}

func validateTextCallback(body callbackBody) error {
	if body.From.ID == "" {
		return fmt.Errorf("decode callback: missing user id")
	}
	switch body.ChatType {
	case "single":
		return nil
	case "group":
		if body.ChatID == "" {
			return fmt.Errorf("decode callback: missing group chat id")
		}
		return nil
	default:
		return fmt.Errorf("decode callback: unsupported chat type %q", body.ChatType)
	}
}
