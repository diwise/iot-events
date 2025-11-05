package mediator

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/matryer/is"
)

const TimeUntilMessagesHaveBeenProcessed time.Duration = 100 * time.Millisecond

func TestThatTheInnerLoopStopsWhenContextIsDone(t *testing.T) {
	_, ctx, _ := testSetup(t)
	ctx.Done()
}

func TestPublishToValidSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)

	valid := NewSubscriber([]string{"default"})
	invalid := NewSubscriber([]string{"unknown"})

	validCalls := &atomic.Int32{}
	invalidCalls := &atomic.Int32{}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-valid.Mailbox():
				validCalls.Add(1)
			case <-invalid.Mailbox():
				invalidCalls.Add(1)
			}
		}
	}()

	m.Register(valid)
	m.Register(invalid)

	time.Sleep(TimeUntilMessagesHaveBeenProcessed)

	m.Publish(NewMessage(context.Background(), "id", "message.type", "default", []byte("{}")))

	time.Sleep(TimeUntilMessagesHaveBeenProcessed)

	is.Equal(int32(1), validCalls.Load())
	is.Equal(int32(0), invalidCalls.Load())

	ctx.Done()
}

func testSetup(t *testing.T) (*is.I, context.Context, Mediator) {
	is := is.New(t)
	ctx := context.Background()

	ctx = logging.NewContextWithLogger(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))

	m := New(ctx)

	go m.Start(ctx)

	return is, ctx, m
}
