package wecomaibot

import (
	"testing"

	"github.com/futuretea/go-wecom-aibot/internal/protocol"
)

func TestMessageFromCallbackNormalizesSingleChatID(t *testing.T) {
	callback := protocol.Callback{
		MessageID: "message",
		BotID:     "bot",
		ChatType:  "single",
		UserID:    "user",
		Text:      "hello",
		Quote:     &protocol.Quote{Type: "text", Text: "previous"},
	}

	message, err := messageFromCallback("session", "request", callback)
	if err != nil {
		t.Fatalf("messageFromCallback() error = %v", err)
	}
	if message.ChatID != "user" || message.ChatType != ChatTypeSingle {
		t.Fatalf("unexpected chat: %#v", message)
	}
	if message.TextContent() != "hello" {
		t.Fatalf("TextContent() = %q, want hello", message.TextContent())
	}
	if message.Quote == nil || message.Quote.Text != "previous" {
		t.Fatalf("unexpected quote: %#v", message.Quote)
	}
	if message.requestID != "request" || message.sessionID != "session" {
		t.Fatalf("message lost internal ownership: %#v", message)
	}
}

func TestMessageFromCallbackRejectsInvalidGroup(t *testing.T) {
	_, err := messageFromCallback("session", "request", protocol.Callback{
		MessageID: "message",
		ChatType:  "group",
		UserID:    "user",
		Text:      "hello",
	})
	if err == nil {
		t.Fatal("messageFromCallback() error = nil, want error")
	}
}
