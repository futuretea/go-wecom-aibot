package wecomaibot

import (
	"context"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

const defaultEndpoint = "wss://openws.work.weixin.qq.com"

const (
	canonicalWebSocketKeyHeader     = "Sec-Websocket-Key"
	canonicalWebSocketVersionHeader = "Sec-Websocket-Version"
	wireWebSocketKeyHeader          = "Sec-WebSocket-Key"
	wireWebSocketVersionHeader      = "Sec-WebSocket-Version"
)

type connector interface {
	Dial(context.Context) (connection, error)
}

type connection interface {
	Read(context.Context) ([]byte, error)
	Write(context.Context, []byte) error
	Close() error
}

type websocketConnector struct {
	endpoint string
}

func (c websocketConnector) Dial(ctx context.Context) (connection, error) {
	httpClient := &http.Client{
		Transport: weComWebSocketTransport{base: http.DefaultTransport},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	options := &websocket.DialOptions{HTTPClient: httpClient}
	conn, response, err := websocket.Dial(ctx, c.endpoint, options)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	return websocketConnection{conn: conn}, nil
}

// weComWebSocketTransport preserves the header spelling required by the
// WeCom endpoint, which rejects Go's otherwise equivalent canonical spelling.
type weComWebSocketTransport struct {
	base http.RoundTripper
}

func (t weComWebSocketTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header = request.Header.Clone()
	preserveHeaderCase(request.Header, canonicalWebSocketKeyHeader, wireWebSocketKeyHeader)
	preserveHeaderCase(request.Header, canonicalWebSocketVersionHeader, wireWebSocketVersionHeader)
	return t.base.RoundTrip(request)
}

func preserveHeaderCase(header http.Header, canonicalName, wireName string) {
	values, ok := header[canonicalName]
	if !ok {
		return
	}
	delete(header, canonicalName)
	header[wireName] = values
}

type websocketConnection struct {
	conn *websocket.Conn
}

func (c websocketConnection) Read(ctx context.Context) ([]byte, error) {
	messageType, data, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("unexpected WebSocket message type %d", messageType)
	}
	return data, nil
}

func (c websocketConnection) Write(ctx context.Context, data []byte) error {
	return c.conn.Write(ctx, websocket.MessageText, data)
}

func (c websocketConnection) Close() error {
	return c.conn.CloseNow()
}
