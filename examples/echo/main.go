package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	wecomaibot "github.com/futuretea/go-wecom-aibot"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client, err := wecomaibot.NewClient(wecomaibot.Config{
		BotID:  os.Getenv("WECOM_BOT_ID"),
		Secret: os.Getenv("WECOM_BOT_SECRET"),
		OnRetry: func(err error, nextDelay time.Duration) {
			log.Printf("connection retry in %s: %v", nextDelay, err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	err = client.Run(ctx, wecomaibot.HandlerFunc(
		func(ctx context.Context, message *wecomaibot.Message) error {
			return client.ReplyMarkdown(ctx, message, "收到消息")
		},
	))
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
