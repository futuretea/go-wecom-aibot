package protocol

import "encoding/json"

type request struct {
	Command string  `json:"cmd"`
	Headers headers `json:"headers"`
	Body    any     `json:"body,omitempty"`
}

// EncodeSubscribe builds an authentication request.
func EncodeSubscribe(requestID, botID, secret string) ([]byte, error) {
	return encode(requestID, "aibot_subscribe", map[string]any{
		"bot_id": botID,
		"secret": secret,
	})
}

// EncodePing builds an application heartbeat request.
func EncodePing(requestID string) ([]byte, error) {
	return encode(requestID, "ping", nil)
}

// EncodeReplyMarkdown builds a passive Markdown reply.
func EncodeReplyMarkdown(requestID, content string) ([]byte, error) {
	return encode(requestID, "aibot_respond_msg", markdownBody(content))
}

// EncodeReplyStream builds one passive stream update.
func EncodeReplyStream(requestID, streamID, content string, finish bool) ([]byte, error) {
	return encode(requestID, "aibot_respond_msg", map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      streamID,
			"content": content,
			"finish":  finish,
		},
	})
}

// EncodeSendMarkdown builds a proactive Markdown message.
func EncodeSendMarkdown(
	requestID string,
	chatID string,
	chatType uint32,
	content string,
) ([]byte, error) {
	body := markdownBody(content)
	body["chatid"] = chatID
	body["chat_type"] = chatType
	return encode(requestID, "aibot_send_msg", body)
}

func markdownBody(content string) map[string]any {
	return map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]any{
			"content": content,
		},
	}
}

func encode(requestID, command string, body any) ([]byte, error) {
	return json.Marshal(request{
		Command: command,
		Headers: headers{RequestID: requestID},
		Body:    body,
	})
}
