package cloudevents

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-events/internal/infrastructure/mediator"
	"github.com/matryer/is"
)

// REV-007: a subscriber handoff to an exited receiver must abort on
// message context cancellation instead of hanging the dispatcher.
func TestSubscriberHandleAbortsOnCanceledMessageCtx(t *testing.T) {
	is := is.New(t)

	sub := &cloudEventSubscriber{
		inbox:       make(chan mediator.Message),
		messageType: "device.statusUpdated",
	}

	msgCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() {
		done <- sub.Handle(mediator.NewMessage(msgCtx, "id", "device.statusUpdated", "default", []byte("{}")))
	}()

	select {
	case handled := <-done:
		is.True(!handled)
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber handoff blocked after receiver exit")
	}
}

// REV-007: a full retry queue must not hang the subscriber loop at
// shutdown; the event is dropped once the context is done.
func TestEnqueueDropsOnCanceledContext(t *testing.T) {
	is := is.New(t)

	q := newFailedEventQueue(func(ctx context.Context, evt eventInfo) error {
		return nil
	})

	for range 1024 {
		q.enqueue(context.Background(), retryEvent{}.info)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		q.enqueue(ctx, retryEvent{}.info)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue blocked on full queue after shutdown")
	}

	is.True(true)
}
