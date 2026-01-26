package cloudevents

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

func TestCloudEventSubscriberHandleDoesNotBlockOnFullInbox(t *testing.T) {
	is := is.New(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := logging.NewContextWithLogger(context.Background(), logger)

	sub := &cloudEventSubscriber{
		id:          "sub",
		inbox:       make(chan mediator.Message, subscriberInboxBuffer),
		messageType: "event",
	}
	msg := mediator.NewMessage(ctx, "id", "event", "tenant", []byte("{}"))

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
