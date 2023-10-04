package mediator

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matryer/is"
)

const TimeUntilMessagesHaveBeenProcessed time.Duration = 100 * time.Millisecond

func TestThatTheInnerLoopStopsWhenContextIsDone(t *testing.T) {
	_, ctx, _ := testSetup(t)
	ctx.Done()
}

func TestThatSubscriberCountIsZeroAtStartup(t *testing.T) {
	is, ctx, m := testSetup(t)
	is.Equal(0, m.SubscriberCount())
	ctx.Done()
}

func TestRegisterSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)

	impl := m.(*mediatorImpl)

	s := NewSubscriber([]string{"default"})
	m.Register(s)
	time.Sleep(TimeUntilMessagesHaveBeenProcessed)

	is.Equal(1, impl.SubscriberCount())

	ctx.Done()
}

func TestUnregisterSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)
	impl := m.(*mediatorImpl)
	s := NewSubscriber([]string{"default"})

	m.Register(s)
	is.Equal(1, impl.SubscriberCount())

	m.Unregister(s)
	is.Equal(0, impl.SubscriberCount())

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

	m.Publish(ctx, NewMessage("id", "message.type", "default", []byte("{}")))

	time.Sleep(TimeUntilMessagesHaveBeenProcessed)

	is.Equal(int32(1), validCalls.Load())
	is.Equal(int32(0), invalidCalls.Load())

	ctx.Done()
}

func testSetup(t *testing.T) (*is.I, context.Context, Mediator) {
	is := is.New(t)
	m := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	go m.Start(ctx)

	return is, ctx, m
}
