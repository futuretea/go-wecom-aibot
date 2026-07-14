# go-wecom-aibot

`go-wecom-aibot` 是企业微信“智能机器人”长连接 API 的 Go SDK。应用只需提供 BotID、Secret 和消息处理函数，即可接收文本消息、回复 Markdown、更新流式消息，以及主动发送 Markdown。

本项目对接的是企业微信智能机器人的 WebSocket API，不是传统群机器人 Webhook，也不是自建应用的 HTTP 回调。

## 功能

- 固定连接企业微信官方 `wss://openws.work.weixin.qq.com` 端点，并拒绝握手重定向。
- 自动订阅、30 秒应用心跳和 15 秒请求超时。
- 传输错误按 `2s, 5s, 10s, 30s, 60s` 退避重连。
- 解析文本、引用、单聊和群聊消息。
- 支持 Markdown 回复、调用者管理的流式回复和 Markdown 主动推送。
- 一个 reader 关联所有响应；handler 可并发运行，固定上限为 16。
- 提供可由 `errors.Is` 和 `errors.As` 判断的状态、协议、连接、处理器和企业微信 API 错误。

当前最低版本为 Go 1.25.12。项目只发布源码库；请使用仍受 Go 官方支持且包含最新安全修复的工具链构建应用。

## 安装

```bash
go get github.com/futuretea/go-wecom-aibot
```

## 快速开始

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
			return client.ReplyMarkdown(ctx, message, "收到消息")
		},
	))
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

完整的可取消示例见 [`examples/echo`](examples/echo)。凭证只应通过环境变量或密钥管理系统提供，不要写入源码、日志或错误包装文本。

`Message.Text` 是不可信外部输入。若要把它嵌入 Markdown 回复，请由业务代码按自己的允许语法做转义或过滤；SDK 不会自动改变调用者提供的 Markdown。

## 流式回复

流 ID 由调用者生成并管理。同一条消息的更新必须串行调用、复用同一非空 ID，并在 10 分钟内用 `Finish: true` 结束：

```go
err := client.ReplyStream(ctx, message, wecomaibot.StreamUpdate{
	ID:      "answer-42",
	Content: "正在生成……",
	Finish:  false,
})
```

SDK 不缓存流内容、不自动生成 ID，也不自动发送最后一次更新。

## 主动推送

目标类型必须明确指定，SDK 不猜测 ID 类型：

```go
err := client.SendMarkdown(ctx, wecomaibot.Target{
	ID:       "zhangsan",
	ChatType: wecomaibot.ChatTypeSingle,
}, "任务已经完成")
```

单聊目标 ID 是企业微信 `userid`，群聊目标 ID 是回调中的 `chatid`。

## 生命周期与错误

- 同一个 `Client` 同时只能运行一次 `Run`；并发调用返回 `ErrAlreadyRunning`。
- 首次订阅成功前、重连间隙和 `Run` 返回后，发送方法返回 `ErrNotConnected`。
- `Message` 绑定接收它的连接。重连后使用旧消息回复会返回 `ErrStaleMessage`。
- 同一回调 `req_id` 只允许一个在途回复；并发回复返回 `ErrRequestInFlight`，收到响应后可以顺序更新流消息。
- 传输错误自动重连，并在等待前同步调用可选的 `Config.OnRetry`。
- 鉴权错误、未知或畸形协议帧、连接替代、handler 错误和 handler 过载会终止 `Run`。
- 取消传给 `Run` 的 context 会关闭连接、取消 handler，并等待已启动的 handler 返回。handler 必须遵守 context 取消。

可以使用 `errors.As` 读取 `*APIError`、`*ProtocolError`、`*ConnectionError` 和 `*HandlerError`，使用 `errors.Is` 判断公开哨兵错误。

## 当前边界

- 只把文本消息交给 handler。图片、图文混排、语音、文件、视频和已知非目标事件会被忽略。
- 不包含媒体上传、模板卡片、消息去重、持久化、访问控制、AI 模型调用或多 Bot 管理。
- 企业微信每个机器人同时只允许一个有效长连接；`disconnected_event` 会直接终止，不参与重连争抢。
- 回复超时后不要继续复用同一条消息发送流更新。本版本无法区分同一 `req_id` 的迟到响应属于哪一次顺序调用；迟到或无法匹配的响应可能终止当前连接。
- 企业微信服务端的真实时序、限流和响应恰好一次语义无法由本地假传输测试证明。

真实企业微信凭证联调不属于当前自动化测试范围。2026-07-14 已完成人工联调：`live-integration: passed`，覆盖文本接收、Markdown 回复、Markdown 主动推送和流式回复结束。该结果是时点证据，不代表平台时序、限流或响应恰好一次语义得到持续保证。

## 许可证

[MIT](LICENSE)
