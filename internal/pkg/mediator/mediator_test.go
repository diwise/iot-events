package mediator

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/matryer/is"
)

func TestThatTheInnerLoopStopsWhenContextIsDone(t *testing.T) {
	testSetup(t)
}

func TestPublishToValidSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)

	valid := NewSubscriber([]string{"default"})
	invalid := NewSubscriber([]string{"unknown"})

	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-valid.Mailbox():
				is.Equal("id", m.ID())
				done <- struct{}{}
			case <-invalid.Mailbox():
				t.Fail()
			}
		}
	}()

	m.Register(valid)
	m.Register(invalid)

	m.Publish(NewMessage(ctx, "id", "message.type", "default", []byte("{}")))

	<-done
}

func testSetup(t *testing.T) (*is.I, context.Context, Mediator) {
	is := is.New(t)

	ctx := logging.NewContextWithLogger(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	
	m := New(ctx)
	m.Start(ctx)

	return is, ctx, m
}
