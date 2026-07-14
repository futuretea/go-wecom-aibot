package wecomaibot

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSendMethodsRequireActiveSession(t *testing.T) {
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	message := &Message{requestID: "request", sessionID: "session"}

	if err := client.ReplyMarkdown(context.Background(), message, "reply"); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("ReplyMarkdown() error = %v, want ErrNotConnected", err)
	}
	if err := client.ReplyStream(context.Background(), message, StreamUpdate{ID: "stream", Content: "part"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("ReplyStream() error = %v, want ErrNotConnected", err)
	}
	if err := client.SendMarkdown(context.Background(), Target{ID: "user", ChatType: ChatTypeSingle}, "send"); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("SendMarkdown() error = %v, want ErrNotConnected", err)
	}
}

func TestReplyStreamReusesCallbackRequestID(t *testing.T) {
	client, conn, message, cancel, runResult := startClientWithMessage(t)
	defer stopRunningClient(t, cancel, runResult)

	streamResult := make(chan error, 1)
	go func() {
		streamResult <- client.ReplyStream(context.Background(), message, StreamUpdate{
			ID:      "stream-1",
			Content: "partial",
			Finish:  false,
		})
	}()
	streamRequest := readRequest(t, conn)
	if streamRequest["cmd"] != "aibot_respond_msg" || requestID(t, streamRequest) != "callback-send" {
		t.Fatalf("unexpected stream request: %#v", streamRequest)
	}
	streamBody := streamRequest["body"].(map[string]any)["stream"].(map[string]any)
	if streamBody["id"] != "stream-1" || streamBody["finish"] != false {
		t.Fatalf("unexpected stream body: %#v", streamBody)
	}
	respondOK(conn, "callback-send")
	if err := <-streamResult; err != nil {
		t.Fatalf("ReplyStream() error = %v", err)
	}
	streamResult = make(chan error, 1)
	go func() {
		streamResult <- client.ReplyStream(context.Background(), message, StreamUpdate{
			ID:      "stream-1",
			Content: "complete",
			Finish:  true,
		})
	}()
	secondStream := readRequest(t, conn)
	if requestID(t, secondStream) != "callback-send" {
		t.Fatalf("second stream req_id = %q, want callback-send", requestID(t, secondStream))
	}
	secondBody := secondStream["body"].(map[string]any)["stream"].(map[string]any)
	if secondBody["finish"] != true {
		t.Fatalf("second stream finish = %v, want true", secondBody["finish"])
	}
	respondOK(conn, "callback-send")
	if err := <-streamResult; err != nil {
		t.Fatalf("second ReplyStream() error = %v", err)
	}
}

func TestSendMarkdownMatchesWireContract(t *testing.T) {
	assertSendMarkdownTarget(t, Target{ID: "group-1", ChatType: ChatTypeGroup}, float64(2))
	assertSendMarkdownTarget(t, Target{ID: "user-1", ChatType: ChatTypeSingle}, float64(1))
}

func assertSendMarkdownTarget(t *testing.T, target Target, chatType float64) {
	t.Helper()
	client, conn, _, cancel, runResult := startClientWithMessage(t)
	defer stopRunningClient(t, cancel, runResult)

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- client.SendMarkdown(
			context.Background(),
			target,
			"notice",
		)
	}()
	sendRequest := readRequest(t, conn)
	if sendRequest["cmd"] != "aibot_send_msg" {
		t.Fatalf("send cmd = %v, want aibot_send_msg", sendRequest["cmd"])
	}
	sendBody := sendRequest["body"].(map[string]any)
	if sendBody["chatid"] != target.ID || sendBody["chat_type"] != chatType {
		t.Fatalf("unexpected send target: %#v", sendBody)
	}
	respondOK(conn, requestID(t, sendRequest))
	if err := <-sendResult; err != nil {
		t.Fatalf("SendMarkdown() error = %v", err)
	}
}

func TestReplyRejectsConcurrentRequestID(t *testing.T) {
	client, conn, message, cancel, runResult := startClientWithMessage(t)
	defer stopRunningClient(t, cancel, runResult)

	first := make(chan error, 1)
	go func() {
		first <- client.ReplyMarkdown(context.Background(), message, "first")
	}()
	request := readRequest(t, conn)

	err := client.ReplyMarkdown(context.Background(), message, "second")
	if !errors.Is(err, ErrRequestInFlight) {
		t.Fatalf("second ReplyMarkdown() error = %v, want ErrRequestInFlight", err)
	}
	respondOK(conn, requestID(t, request))
	if err := <-first; err != nil {
		t.Fatalf("first ReplyMarkdown() error = %v", err)
	}
}

func TestReplyRejectsMessageFromPreviousRun(t *testing.T) {
	firstConn := newFakeConnection()
	secondConn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{firstConn, secondConn}}

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstMessage := make(chan *Message, 1)
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- client.Run(firstCtx, HandlerFunc(func(ctx context.Context, message *Message) error {
			firstMessage <- message
			<-ctx.Done()
			return nil
		}))
	}()
	respondOK(firstConn, requestID(t, readRequest(t, firstConn)))
	firstConn.reads <- fakeRead{data: textCallback("old-request", "old-message")}
	oldMessage := <-firstMessage
	firstCancel()
	<-firstResult

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondReady := make(chan struct{}, 1)
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- client.Run(secondCtx, HandlerFunc(func(context.Context, *Message) error {
			secondReady <- struct{}{}
			return nil
		}))
	}()
	respondOK(secondConn, requestID(t, readRequest(t, secondConn)))
	secondConn.reads <- fakeRead{data: textCallback("new-request", "new-message")}
	<-secondReady

	if err := client.ReplyMarkdown(context.Background(), oldMessage, "stale"); !errors.Is(err, ErrStaleMessage) {
		t.Fatalf("ReplyMarkdown() error = %v, want ErrStaleMessage", err)
	}
	secondCancel()
	<-secondResult
}

func TestSendMethodsValidatePublicInput(t *testing.T) {
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	if err := client.SendMarkdown(context.Background(), Target{ID: "target", ChatType: "unknown"}, "content"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SendMarkdown() error = %v, want ErrInvalidArgument", err)
	}
	if err := client.ReplyStream(context.Background(), nil, StreamUpdate{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ReplyStream() error = %v, want ErrInvalidArgument", err)
	}
}

func startClientWithMessage(t *testing.T) (*Client, *fakeConnection, *Message, context.CancelFunc, <-chan error) {
	t.Helper()
	conn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{conn}}
	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan *Message, 1)
	runResult := make(chan error, 1)
	go func() {
		runResult <- client.Run(ctx, HandlerFunc(func(ctx context.Context, message *Message) error {
			messages <- message
			<-ctx.Done()
			return nil
		}))
	}()
	respondOK(conn, requestID(t, readRequest(t, conn)))
	conn.reads <- fakeRead{data: textCallback("callback-send", "message-send")}
	select {
	case message := <-messages:
		return client, conn, message, cancel, runResult
	case <-time.After(time.Second):
		cancel()
		t.Fatal("handler did not receive message")
		return nil, nil, nil, nil, nil
	}
}

func stopRunningClient(t *testing.T, cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop")
	}
}
