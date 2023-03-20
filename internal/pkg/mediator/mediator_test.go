package mediator

import (
	"context"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

var SleepForMs time.Duration = 100 * time.Millisecond

func TestThatTheInnerLoopStopsWhenContextIsDone(t *testing.T) {
	_, ctx, _ := testSetup(t)
	ctx.Done()
}

func TestRegisterSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)

	impl := m.(*mediatorImpl)
	is.Equal(0, len(impl.subscribers))

	s := NewSubscriber([]string{"default"})
	m.Register(s)
	time.Sleep(SleepForMs)

	is.Equal(1, len(impl.subscribers))

	ctx.Done()
}

func TestUnregisterSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)

	impl := m.(*mediatorImpl)
	is.Equal(0, len(impl.subscribers))

	s := NewSubscriber([]string{"default"})
	m.Register(s)

	is.Equal(1, len(impl.subscribers))

	m.Unregister(s)
	time.Sleep(SleepForMs) // TODO: ...

	is.Equal(0, len(impl.subscribers))

	ctx.Done()
}

func TestPublishToValidSubscribers(t *testing.T) {
	is, ctx, m := testSetup(t)

	impl := m.(*mediatorImpl)
	is.Equal(0, len(impl.subscribers))

	valid := NewSubscriber([]string{"default"})
	invalid := NewSubscriber([]string{"unknown"})

	var validCalls = 0
	var invalidCalls = 0

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-valid.Mailbox():
				validCalls++
			case <-invalid.Mailbox():
				invalidCalls++
			}
		}
	}()

	m.Register(valid)
	m.Register(invalid)

	time.Sleep(SleepForMs)

	m.Publish(NewMessage("id", "message.type", "default", []byte("{}")))

	time.Sleep(SleepForMs)

	is.Equal(1, validCalls)
	is.Equal(0, invalidCalls)

	ctx.Done()
}

func testSetup(t *testing.T) (*is.I, context.Context, Mediator) {
	is := is.New(t)
	m := New(zerolog.Logger{})
	ctx := context.Background()

	go m.Start(ctx)

	return is, ctx, m
}
