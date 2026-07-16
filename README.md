# go-wecom-aibot

English | [简体中文](README.zh-CN.md)

`go-wecom-aibot` is a Go SDK for the WeCom (WeChat Work) "AI Bot" long-connection API. Give it a BotID, a Secret, and a message handler, and you can receive text messages, reply with Markdown, drive streaming replies, and proactively push Markdown — no HTTP server or public callback endpoint required.

This SDK targets the WebSocket API of WeCom AI Bots. It is **not** the group-bot Webhook API, and **not** the HTTP callback of a self-built WeCom application.

## Features

- Connects to the official `wss://openws.work.weixin.qq.com` endpoint and refuses handshake redirects.
- Handles subscription, a 30-second application heartbeat, and 15-second request timeouts for you.
- Reconnects transport failures with a `2s, 5s, 10s, 30s, 60s` backoff.
- Parses text, quote, direct (single) and group chat messages.
- Markdown replies, caller-managed streaming replies, and proactive Markdown push.
- One reader correlates all responses; handlers run concurrently, capped at 16.
- Status, protocol, connection, handler, and WeCom API errors work with `errors.Is` and `errors.As`.

Requires Go 1.25.12 or later. The project ships as a source-only library; build your application with a toolchain that is still supported by the Go team and includes the latest security fixes.

## Installation

```bash
go get github.com/futuretea/go-wecom-aibot
```

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"

	wecomaibot "github.com/futuretea/go-wecom-aibot"
)

func run(ctx context.Context) error {
	client, err := wecomaibot.NewClient(wecomaibot.Config{
		BotID:  os.Getenv("WECOM_BOT_ID"),
		Secret: os.Getenv("WECOM_BOT_SECRET"),
	})
	if err != nil {
		return err
	}

	return client.Run(ctx, wecomaibot.HandlerFunc(
		func(ctx context.Context, message *wecomaibot.Message) error {
			return client.ReplyMarkdown(ctx, message, "Got your message")
		},
	))
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

See [`examples/echo`](examples/echo) for a complete, cancellable example. Supply credentials only through environment variables or a secret manager — never in source code, logs, or error wrapping text.

`Message.Text` is untrusted external input. If you embed it into a Markdown reply, escape or filter it in your own code according to the syntax you allow; the SDK does not transform caller-provided Markdown.

## Streaming replies

Stream IDs are generated and owned by you. Updates for one message must be called serially, reuse the same non-empty ID, and finish within 10 minutes with `Finish: true`:

```go
err := client.ReplyStream(ctx, message, wecomaibot.StreamUpdate{
	ID:      "answer-42",
	Content: "Generating...",
	Finish:  false,
})
```

The SDK does not buffer stream content, generate IDs for you, or send the final update automatically.

## Proactive messages

The target type must be stated explicitly — the SDK never guesses the ID type:

```go
err := client.SendMarkdown(ctx, wecomaibot.Target{
	ID:       "zhangsan",
	ChatType: wecomaibot.ChatTypeSingle,
}, "Task completed")
```

A direct-chat target ID is a WeCom `userid`; a group-chat target ID is the `chatid` from callbacks.

## Lifecycle and errors

- A `Client` runs `Run` once at a time; concurrent calls return `ErrAlreadyRunning`.
- Before the first successful subscription, during reconnect gaps, and after `Run` returns, send methods return `ErrNotConnected`.
- A `Message` is bound to the connection that received it. Replying with an old message after a reconnect returns `ErrStaleMessage`.
- One callback `req_id` allows only one in-flight reply; concurrent replies return `ErrRequestInFlight`. After a response arrives, you may update the stream message sequentially.
- Transport errors reconnect automatically, calling the optional `Config.OnRetry` synchronously before waiting.
- Authentication errors, unknown or malformed protocol frames, connection replacement, handler errors, and handler overload terminate `Run`.
- Canceling the context passed to `Run` closes the connection, cancels handlers, and waits for started handlers to return. Handlers must respect context cancellation.

Use `errors.As` to read `*APIError`, `*ProtocolError`, `*ConnectionError`, and `*HandlerError`; use `errors.Is` for the exported sentinel errors.

## Current boundaries

- Only text messages reach your handler. Images, mixed text-image posts, voice, files, video, and known non-target events are ignored.
- No media upload, template cards, message deduplication, persistence, access control, AI model calls, or multi-bot management.
- WeCom allows only one active long connection per bot at a time; a `disconnected_event` terminates the run immediately and does not compete in reconnects.
- After a reply times out, do not keep reusing the same message for stream updates. This version cannot tell which sequential call a late response for the same `req_id` belongs to; a late or unmatched response may terminate the current connection.
- The real timing, rate limiting, and exactly-once response semantics of the WeCom server cannot be proven by local fake-transport tests.

Real-credential integration is outside the automated test scope. A manual live check was completed on 2026-07-14: `live-integration: passed`, covering text receive, Markdown reply, proactive Markdown push, and stream finish. That result is point-in-time evidence and does not guarantee platform timing, rate limiting, or exactly-once response semantics on an ongoing basis.

## License

[MIT](LICENSE)
