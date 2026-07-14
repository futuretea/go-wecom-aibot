package wecomaibot

import (
	"errors"
	"strings"
	"testing"
)

func TestNewClientValidatesCredentialsWithoutLeakingSecret(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{name: "missing bot id", config: Config{Secret: "sentinel-secret"}},
		{name: "missing secret", config: Config{BotID: "bot-id"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tt.config)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewClient() error = %v, want ErrInvalidConfig", err)
			}
			if strings.Contains(err.Error(), "sentinel-secret") {
				t.Fatalf("error leaked Secret: %v", err)
			}
		})
	}
}

func TestExternalErrorStringsEscapeControlCharacters(t *testing.T) {
	apiErr := &APIError{Code: 1, Message: "line one\n\x1b[31mline two"}
	if strings.ContainsAny(apiErr.Error(), "\n\r\x1b") {
		t.Fatalf("APIError.Error() contains raw control characters: %q", apiErr.Error())
	}
	if apiErr.Message != "line one\n\x1b[31mline two" {
		t.Fatalf("APIError.Message lost raw value: %q", apiErr.Message)
	}

	cause := errors.New("closed\r\nforged")
	connectionErr := &ConnectionError{Err: cause}
	if strings.ContainsAny(connectionErr.Error(), "\n\r\x1b") {
		t.Fatalf("ConnectionError.Error() contains raw control characters: %q", connectionErr.Error())
	}
	if !errors.Is(connectionErr, cause) {
		t.Fatal("ConnectionError lost unwrap cause")
	}
}

func TestNewClientAcceptsCredentials(t *testing.T) {
	client, err := NewClient(Config{BotID: "bot-id", Secret: "secret"})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if client == nil {
		t.Fatal("NewClient() returned nil client")
	}
}
