package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// storageCloseFunc adapts a func to the Close() method set ownedResources
// requires, so shutdown order can be verified with fakes.
type storageCloseFunc func()

func (f storageCloseFunc) Close() { f() }

// REV-007: shutdown must stop inflow, await admitted handlers, cancel
// workers and then release storage exactly once, in messenger ->
// cancel -> storage order, even when invoked twice. messenger.Close on
// a real context is not safe to call twice, hence the guard.
func TestShutdownIsOrderedAndIdempotent(t *testing.T) {
	is := is.New(t)

	var order []string
	storageCloses := 0
	cancels := 0
	messenger := &messaging.MsgContextMock{
		CloseFunc: func() { order = append(order, "messenger") },
	}

	owned := &ownedResources{
		messenger: messenger,
		tracker:   &handlerTracker{},
		cancel: func() {
			cancels++
			order = append(order, "cancel")
		},
		storage: storageCloseFunc(func() {
			storageCloses++
			order = append(order, "storage")
		}),
	}

	ctx := context.Background()
	owned.close(ctx)
	owned.close(ctx)

	is.Equal(cancels, 1)
	is.Equal(storageCloses, 1)
	is.Equal(order, []string{"messenger", "cancel", "storage"})
}

// REV-007: tracked handlers are awaited within budget; wait reports
// whether all admitted deliveries finished.
func TestHandlerTrackerWaitsForInflight(t *testing.T) {
	is := is.New(t)

	tracker := &handlerTracker{}
	release := make(chan struct{})
	handlerStarted := make(chan struct{})

	tracked := tracker.track(func(context.Context, messaging.IncomingTopicMessage, *slog.Logger) {
		close(handlerStarted)
		<-release
	})

	done := make(chan bool, 1)
	go func() {
		tracked(context.Background(), nil, slog.Default())
	}()

	<-handlerStarted
	go func() { done <- tracker.wait(5 * time.Second) }()

	select {
	case <-done:
		t.Fatal("wait returned while handler still blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	is.True(<-done)
}

// REV-007: wait times out instead of hanging shutdown forever.
func TestHandlerTrackerWaitTimesOut(t *testing.T) {
	is := is.New(t)

	tracker := &handlerTracker{}
	tracker.wg.Add(1)
	defer tracker.wg.Done()

	is.True(!tracker.wait(20 * time.Millisecond))
}

// BASE-008: shutdown with no initialized resources (e.g. failed OnInit)
// must be a safe no-op.
func TestShutdownWithoutResourcesIsSafe(t *testing.T) {
	owned := &ownedResources{}

	owned.close(context.Background())
	owned.close(context.Background())
}

// BASE-009: readiness stubs always report OK without touching any
// dependency, even with no initialized clients.
func TestReadinessStubsAlwaysOK(t *testing.T) {
	is := is.New(t)

	probes := readinessProbes()
	is.Equal(len(probes), 3)

	for _, name := range []string{"rabbitmq", "mqtt", "timescale"} {
		status, err := probes[name](context.Background())
		is.NoErr(err)
		is.Equal(status, "ok")
	}
}
