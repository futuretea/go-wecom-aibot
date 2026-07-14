package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRunHandlerCanWaitForMarkdownReply(t *testing.T) {
	conn := newFakeConnection()
	client, err := NewClient(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.connector = &fakeConnector{connections: []connection{conn}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handled := make(chan error, 1)
	runResult := make(chan error, 1)
	go func() {
		runResult <- client.Run(ctx, HandlerFunc(func(ctx context.Context, message *Message) error {
			err := client.ReplyMarkdown(ctx, message, "received")
			handled <- err
			return err
		}))
	}()

	subscribe := readRequest(t, conn)
	respondOK(conn, requestID(t, subscribe))
	conn.reads <- fakeRead{data: textCallback("callback-1", "message-1")}

	reply := readRequest(t, conn)
	if reply["cmd"] != "aibot_respond_msg" {
		t.Fatalf("reply cmd = %v, want aibot_respond_msg", reply["cmd"])
	}
	if got := requestID(t, reply); got != "callback-1" {
		t.Fatalf("reply req_id = %q, want callback-1", got)
	}
	respondOK(conn, "callback-1")

	select {
	case err := <-handled:
		if err != nil {
			t.Fatalf("handler reply error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler blocked while waiting for reply response")
	}

	cancel()
	select {
	case err := <-runResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func TestRunRetriesDialWithCappedBackoff(t *testing.T) {
	client, err := NewClient(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	dialErrors := make([]error, 7)
	for i := range dialErrors {
		dialErrors[i] = errors.New("dial failed")
	}
	client.connector = &fakeConnector{errors: dialErrors}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var delays []time.Duration
	var observed []time.Duration
	client.config.OnRetry = func(_ error, delay time.Duration) {
		observed = append(observed, delay)
	}
	client.waitRetry = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == len(dialErrors) {
			cancel()
			return context.Canceled
		}
		return nil
	}

	err = client.Run(ctx, HandlerFunc(func(context.Context, *Message) error { return nil }))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	want := []time.Duration{
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		60 * time.Second,
		60 * time.Second,
		60 * time.Second,
	}
	if fmt.Sprint(delays) != fmt.Sprint(want) {
		t.Fatalf("retry delays = %v, want %v", delays, want)
	}
	if fmt.Sprint(observed) != fmt.Sprint(want) {
		t.Fatalf("OnRetry delays = %v, want %v", observed, want)
	}
}

func TestRunDoesNotRetrySubscriptionAPIError(t *testing.T) {
	conn := newFakeConnection()
	client, _ := NewClient(Config{
		BotID:  "bot",
		Secret: "secret",
		OnRetry: func(error, time.Duration) {
			t.Error("OnRetry called for subscription API error")
		},
	})
	client.connector = &fakeConnector{connections: []connection{conn}}

	result := make(chan error, 1)
	go func() {
		result <- client.Run(context.Background(), HandlerFunc(func(context.Context, *Message) error {
			return nil
		}))
	}()
	subscribe := readRequest(t, conn)
	requestID := requestID(t, subscribe)
	conn.reads <- fakeRead{data: []byte(`{"headers":{"req_id":"` + requestID + `"},"errcode":40001,"errmsg":"invalid credential"}`)}

	select {
	case err := <-result:
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != 40001 {
			t.Fatalf("Run() error = %v, want APIError 40001", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return subscription API error")
	}
}

func TestRunRejectsConcurrentCall(t *testing.T) {
	conn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{conn}}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		first <- client.Run(ctx, HandlerFunc(func(context.Context, *Message) error { return nil }))
	}()
	respondOK(conn, requestID(t, readRequest(t, conn)))

	err := client.Run(context.Background(), HandlerFunc(func(context.Context, *Message) error { return nil }))
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run() error = %v, want ErrAlreadyRunning", err)
	}
	cancel()
	<-first
}

func TestRunReturnsHandlerError(t *testing.T) {
	conn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{conn}}
	want := errors.New("handler failed")
	result := make(chan error, 1)
	go func() {
		result <- client.Run(context.Background(), HandlerFunc(func(context.Context, *Message) error {
			return want
		}))
	}()
	respondOK(conn, requestID(t, readRequest(t, conn)))
	conn.reads <- fakeRead{data: textCallback("callback-handler", "message-handler")}

	select {
	case err := <-result:
		var handlerErr *HandlerError
		if !errors.As(err, &handlerErr) || !errors.Is(err, want) {
			t.Fatalf("Run() error = %v, want HandlerError", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return handler error")
	}
}

func TestRunStopsOnHandlerOverload(t *testing.T) {
	conn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{conn}}
	result := make(chan error, 1)
	go func() {
		result <- client.Run(context.Background(), HandlerFunc(func(ctx context.Context, _ *Message) error {
			<-ctx.Done()
			return nil
		}))
	}()
	respondOK(conn, requestID(t, readRequest(t, conn)))
	for i := 0; i < handlerLimit+1; i++ {
		conn.reads <- fakeRead{data: textCallback(fmt.Sprintf("callback-%d", i), fmt.Sprintf("message-%d", i))}
	}

	select {
	case err := <-result:
		if !errors.Is(err, ErrHandlerOverload) {
			t.Fatalf("Run() error = %v, want ErrHandlerOverload", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop on handler overload")
	}
}

func TestRunStopsWhenConnectionIsReplaced(t *testing.T) {
	conn := newFakeConnection()
	client, _ := NewClient(Config{BotID: "bot", Secret: "secret"})
	client.connector = &fakeConnector{connections: []connection{conn}}
	result := make(chan error, 1)
	go func() {
		result <- client.Run(context.Background(), HandlerFunc(func(context.Context, *Message) error { return nil }))
	}()
	respondOK(conn, requestID(t, readRequest(t, conn)))
	conn.reads <- fakeRead{data: []byte(`{
		"cmd":"aibot_event_callback",
		"headers":{"req_id":"event-1"},
		"body":{"msgid":"event-message","aibotid":"bot","msgtype":"event","event":{"eventtype":"disconnected_event"}}
	}`)}

	select {
	case err := <-result:
		if !errors.Is(err, ErrConnectionReplaced) {
			t.Fatalf("Run() error = %v, want ErrConnectionReplaced", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop on connection replacement")
	}
}

type fakeConnector struct {
	connections []connection
	errors      []error
	dials       int
}

func (c *fakeConnector) Dial(context.Context) (connection, error) {
	index := c.dials
	c.dials++
	if index < len(c.errors) && c.errors[index] != nil {
		return nil, c.errors[index]
	}
	if index >= len(c.connections) {
		return nil, errors.New("unexpected dial")
	}
	return c.connections[index], nil
}

func readRequest(t *testing.T, conn *fakeConnection) map[string]any {
	t.Helper()
	select {
	case data := <-conn.writes:
		var request map[string]any
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request")
		return nil
	}
}

func requestID(t *testing.T, request map[string]any) string {
	t.Helper()
	headers, ok := request["headers"].(map[string]any)
	if !ok {
		t.Fatalf("request headers = %#v", request["headers"])
	}
	requestID, ok := headers["req_id"].(string)
	if !ok || requestID == "" {
		t.Fatalf("request req_id = %#v", headers["req_id"])
	}
	return requestID
}

func respondOK(conn *fakeConnection, requestID string) {
	conn.reads <- fakeRead{data: []byte(`{"headers":{"req_id":"` + requestID + `"},"errcode":0,"errmsg":"ok"}`)}
}

func textCallback(requestID, messageID string) []byte {
	return []byte(`{
		"cmd":"aibot_msg_callback",
		"headers":{"req_id":"` + requestID + `"},
		"body":{
			"msgid":"` + messageID + `",
			"aibotid":"bot",
			"chattype":"single",
			"from":{"userid":"user-1"},
			"msgtype":"text",
			"text":{"content":"hello"}
		}
	}`)
}
