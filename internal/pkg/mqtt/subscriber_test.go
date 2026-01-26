package mqtt

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/matryer/is"
)

func TestMQTTSubscriberHandleDoesNotBlockOnFullInbox(t *testing.T) {
	is := is.New(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := logging.NewContextWithLogger(context.Background(), logger)

	sub := newSubscriber("topic", func(mediator.Message) {})
	msg := mediator.NewMessage(ctx, "id", "topic", "tenant", []byte("{}"))

	for i := 0; i < subscriberInboxBuffer; i++ {
		sub.inbox <- msg
	}

	done := make(chan struct{})
	go func() {
		sub.Handle(msg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		is.Fail()
	}
}
