package wecomaibot

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestWeComWebSocketTransportPreservesRequiredHeaderCase(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	request.Header[canonicalWebSocketKeyHeader] = []string{"key"}
	request.Header[canonicalWebSocketVersionHeader] = []string{"13"}

	transport := weComWebSocketTransport{base: roundTripperFunc(func(got *http.Request) (*http.Response, error) {
		if values, ok := exactHeaderValues(got.Header, wireWebSocketKeyHeader); !ok || len(values) != 1 || values[0] != "key" {
			t.Fatalf("%s = %q, want key", wireWebSocketKeyHeader, values)
		}
		if values, ok := exactHeaderValues(got.Header, wireWebSocketVersionHeader); !ok || len(values) != 1 || values[0] != "13" {
			t.Fatalf("%s = %q, want 13", wireWebSocketVersionHeader, values)
		}
		if _, ok := got.Header[canonicalWebSocketKeyHeader]; ok {
			t.Fatalf("request still contains %s", canonicalWebSocketKeyHeader)
		}
		if _, ok := got.Header[canonicalWebSocketVersionHeader]; ok {
			t.Fatalf("request still contains %s", canonicalWebSocketVersionHeader)
		}
		return &http.Response{
			StatusCode: http.StatusSwitchingProtocols,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}

	if _, err := transport.RoundTrip(request); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if _, ok := request.Header[canonicalWebSocketKeyHeader]; !ok {
		t.Fatalf("original request lost %s", canonicalWebSocketKeyHeader)
	}
	if _, ok := request.Header[canonicalWebSocketVersionHeader]; !ok {
		t.Fatalf("original request lost %s", canonicalWebSocketVersionHeader)
	}
}

func exactHeaderValues(header http.Header, name string) ([]string, bool) {
	for key, values := range header {
		if key == name {
			return values, true
		}
	}
	return nil, false
}

func TestWebsocketConnectorRejectsRedirect(t *testing.T) {
	var targetVisits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetVisits.Add(1)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint := "ws" + strings.TrimPrefix(source.URL, "http")
	_, err := (websocketConnector{endpoint: endpoint}).Dial(ctx)
	if err == nil {
		t.Fatal("Dial() error = nil, want redirect rejection")
	}
	if got := targetVisits.Load(); got != 0 {
		t.Fatalf("redirect target visits = %d, want 0", got)
	}
}
