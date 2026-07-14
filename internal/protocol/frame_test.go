package protocol

import (
	"encoding/json"
	"testing"
)

func TestDecodeTextCallbackReturnsCallbackDTO(t *testing.T) {
	raw := []byte(`{
		"cmd":"aibot_msg_callback",
		"headers":{"req_id":"req-1"},
		"body":{
			"msgid":"msg-1",
			"aibotid":"bot-1",
			"chatid":"chat-1",
			"chattype":"group",
			"from":{"userid":"user-1"},
			"msgtype":"text",
			"text":{"content":"hello"},
			"quote":{"msgtype":"text","text":{"content":"previous"}}
		}
	}`)

	frame, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	assertTextCallback(t, frame)
}

func assertTextCallback(t *testing.T, frame Frame) {
	t.Helper()
	if frame.Kind != KindTextCallback {
		t.Fatalf("Kind = %q, want %q", frame.Kind, KindTextCallback)
	}
	if frame.RequestID != "req-1" || frame.Callback.MessageID != "msg-1" {
		t.Fatalf("unexpected identifiers: %#v", frame)
	}
	if frame.Callback.ChatID != "chat-1" || frame.Callback.ChatType != "group" {
		t.Fatalf("unexpected chat: %#v", frame.Callback)
	}
	if frame.Callback.UserID != "user-1" || frame.Callback.Text != "hello" {
		t.Fatalf("unexpected message: %#v", frame.Callback)
	}
	if frame.Callback.Quote == nil || frame.Callback.Quote.Text != "previous" {
		t.Fatalf("unexpected quote: %#v", frame.Callback.Quote)
	}
}

func TestDecodeClassifiesKnownAndUnknownCallbacks(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    Kind
		wantErr bool
	}{
		{
			name: "known unsupported image",
			raw:  `{"cmd":"aibot_msg_callback","headers":{"req_id":"r1"},"body":{"msgid":"m1","aibotid":"bot","msgtype":"image"}}`,
			want: KindIgnored,
		},
		{
			name: "known unsupported enter chat",
			raw:  `{"cmd":"aibot_event_callback","headers":{"req_id":"r2"},"body":{"msgid":"m2","aibotid":"bot","msgtype":"event","event":{"eventtype":"enter_chat"}}}`,
			want: KindIgnored,
		},
		{
			name: "connection replaced",
			raw:  `{"cmd":"aibot_event_callback","headers":{"req_id":"r3"},"body":{"msgid":"m3","aibotid":"bot","msgtype":"event","event":{"eventtype":"disconnected_event"}}}`,
			want: KindDisconnected,
		},
		{
			name:    "unknown message type",
			raw:     `{"cmd":"aibot_msg_callback","headers":{"req_id":"r4"},"body":{"msgid":"m4","aibotid":"bot","msgtype":"future_type"}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := Decode([]byte(tt.raw))
			if tt.wantErr {
				if err == nil {
					t.Fatal("Decode() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if frame.Kind != tt.want {
				t.Fatalf("Kind = %q, want %q", frame.Kind, tt.want)
			}
		})
	}
}

func TestDecodeResponsePreservesRequestIDAndAPIError(t *testing.T) {
	frame, err := Decode([]byte(`{
		"headers":{"req_id":"request-42"},
		"errcode":40001,
		"errmsg":"invalid credential"
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if frame.Kind != KindResponse {
		t.Fatalf("Kind = %q, want %q", frame.Kind, KindResponse)
	}
	if frame.RequestID != "request-42" {
		t.Fatalf("RequestID = %q, want request-42", frame.RequestID)
	}
	if frame.ErrorCode != 40001 || frame.ErrorMessage != "invalid credential" {
		t.Fatalf("unexpected API error: %#v", frame)
	}
}

func TestDecodeRejectsCallbacksWithoutBotID(t *testing.T) {
	tests := []string{
		`{"cmd":"aibot_msg_callback","headers":{"req_id":"r1"},"body":{"msgid":"m1","chattype":"single","from":{"userid":"user"},"msgtype":"text","text":{"content":"hello"}}}`,
		`{"cmd":"aibot_msg_callback","headers":{"req_id":"r2"},"body":{"msgid":"m2","msgtype":"image"}}`,
		`{"cmd":"aibot_event_callback","headers":{"req_id":"r3"},"body":{"msgid":"m3","msgtype":"event","event":{"eventtype":"enter_chat"}}}`,
		`{"cmd":"aibot_event_callback","headers":{"req_id":"r4"},"body":{"msgid":"m4","msgtype":"event","event":{"eventtype":"disconnected_event"}}}`,
	}

	for _, raw := range tests {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatal("Decode() error = nil, want missing bot ID error")
		}
	}
}

var encodeRequestTests = []struct {
	name       string
	encode     func() ([]byte, error)
	command    string
	request    string
	assertBody func(*testing.T, map[string]any)
}{
	{
		name:    "subscribe",
		encode:  func() ([]byte, error) { return EncodeSubscribe("r1", "bot", "secret") },
		command: "aibot_subscribe",
		request: "r1",
		assertBody: func(t *testing.T, body map[string]any) {
			t.Helper()
			if body["bot_id"] != "bot" || body["secret"] != "secret" {
				t.Fatalf("unexpected body: %#v", body)
			}
		},
	},
	{
		name:    "ping",
		encode:  func() ([]byte, error) { return EncodePing("r2") },
		command: "ping",
		request: "r2",
	},
	{
		name:       "markdown reply",
		encode:     func() ([]byte, error) { return EncodeReplyMarkdown("r3", "hello") },
		command:    "aibot_respond_msg",
		request:    "r3",
		assertBody: assertMessageBody("markdown", "hello"),
	},
	{
		name: "stream reply",
		encode: func() ([]byte, error) {
			return EncodeReplyStream("r4", "stream-1", "partial", true)
		},
		command: "aibot_respond_msg",
		request: "r4",
		assertBody: func(t *testing.T, body map[string]any) {
			t.Helper()
			stream := body["stream"].(map[string]any)
			if body["msgtype"] != "stream" || stream["id"] != "stream-1" ||
				stream["content"] != "partial" || stream["finish"] != true {
				t.Fatalf("unexpected body: %#v", body)
			}
		},
	},
	{
		name: "proactive markdown",
		encode: func() ([]byte, error) {
			return EncodeSendMarkdown("r5", "chat", 2, "notice")
		},
		command: "aibot_send_msg",
		request: "r5",
		assertBody: func(t *testing.T, body map[string]any) {
			t.Helper()
			if body["chatid"] != "chat" || body["chat_type"] != float64(2) {
				t.Fatalf("unexpected target: %#v", body)
			}
			assertMessageBody("markdown", "notice")(t, body)
		},
	},
}

func TestEncodeRequestsMatchWireContract(t *testing.T) {
	for _, tt := range encodeRequestTests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.encode()
			if err != nil {
				t.Fatalf("encode error = %v", err)
			}
			var wire map[string]any
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if wire["cmd"] != tt.command {
				t.Fatalf("cmd = %v, want %q", wire["cmd"], tt.command)
			}
			headers := wire["headers"].(map[string]any)
			if headers["req_id"] != tt.request {
				t.Fatalf("req_id = %v, want %q", headers["req_id"], tt.request)
			}
			if tt.assertBody != nil {
				tt.assertBody(t, wire["body"].(map[string]any))
			}
		})
	}
}

func assertMessageBody(messageType, content string) func(*testing.T, map[string]any) {
	return func(t *testing.T, body map[string]any) {
		t.Helper()
		markdown := body["markdown"].(map[string]any)
		if body["msgtype"] != messageType || markdown["content"] != content {
			t.Fatalf("unexpected body: %#v", body)
		}
	}
}
