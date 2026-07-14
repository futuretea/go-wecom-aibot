package wecomaibot

import "time"

// Config contains the credentials and optional retry observer.
type Config struct {
	BotID   string
	Secret  string
	OnRetry func(err error, nextDelay time.Duration)
}
