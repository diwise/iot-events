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

func testContext() context.Context {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return logging.NewContextWithLogger(context.Background(), logger)
}

func testClient(t *testing.T, ctx context.Context) *mqttClient {
	t.Helper()

	c, err := NewClient(ctx, Config{Enabled: true, BrokerUrl: "tcp://127.0.0.1:1", ClientId: "test"})
	if err != nil {
		t.Fatalf("failed to create mqtt client: %v", err)
	}

	mc, ok := c.(*mqttClient)
	if !ok {
		t.Fatal("expected *mqttClient")
	}

	return mc
}

// REV-007: a pending retry racing shutdown must be dropped, never sent
// on a closed channel.
func TestRetryAfterCancelDoesNotPanic(t *testing.T) {
	is := is.New(t)

	ctx, cancel := context.WithCancel(testContext())
	c := testClient(t, ctx)
	c.Start(ctx)

	c.errmsg <- &topicMessage{topic: "t", retry: 0}

	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for c.started.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	is.True(!c.started.Load())
}

// REV-007: a connect attempt against an unavailable broker must abort
// on shutdown instead of hanging the worker loop.
func TestConnectToUnavailableBrokerDoesNotBlockShutdown(t *testing.T) {
	is := is.New(t)

	ctx, cancel := context.WithCancel(testContext())
	c := testClient(t, ctx)
	c.Start(ctx)

	done := make(chan error, 1)
	go func() {
		done <- c.Publish(context.Background(), &topicMessage{topic: "t"})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish was not picked up for connect")
	}

	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for c.started.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	is.True(!c.started.Load())
}

// REV-007: publishing after stop must fail promptly instead of hanging
// on a receiver that will never come back.
func TestPublishAfterStopFailsPromptly(t *testing.T) {
	is := is.New(t)

	ctx, cancel := context.WithCancel(testContext())
	c := testClient(t, ctx)
	c.Start(ctx)
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for c.started.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	is.True(!c.started.Load())

	done := make(chan error, 1)
	go func() {
		done <- c.Publish(context.Background(), &topicMessage{topic: "t"})
	}()

	select {
	case err := <-done:
		is.True(err != nil)
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked after stop")
	}
}

// REV-007: a subscriber handoff to an exited receiver must abort on
// message context cancellation instead of hanging the dispatcher.
func TestSubscriberHandleAbortsOnCanceledMessageCtx(t *testing.T) {
	is := is.New(t)

	sub := newSubscriber("message.accepted", func(mediator.Message) {})

	msgCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() {
		done <- sub.Handle(mediator.NewMessage(msgCtx, "id", "message.accepted", "default", []byte("{}")))
	}()

	select {
	case handled := <-done:
		is.True(!handled)
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber handoff blocked after receiver exit")
	}
}
