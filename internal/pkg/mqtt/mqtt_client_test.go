package mqtt

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestPublishCtxCancelDoesNotPanic(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	c := &mqttClient{
		pub:     make(chan TopicMessage, 1),
		errmsg:  make(chan TopicMessage, 1),
		done:    make(chan struct{}),
		started: atomic.Bool{},
	}

	c.started.Store(true)

	// attempt to publish with canceled context
	cancel()
	err := c.Publish(ctx, newTopicMessage("t", "p"))
	is.True(err != nil)

	// ensure drain logic doesn't block
	time.Sleep(10 * time.Millisecond)
}

func TestRetryPublishShutdownDoesNotSendOnClosedChannel(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &mqttClient{
		pub:     make(chan TopicMessage),
		errmsg:  make(chan TopicMessage, 1),
		done:    make(chan struct{}),
		started: atomic.Bool{},
	}

	go c.retryPublish(ctx)

	msg := &topicMessage{topic: "t", payload: "p", retry: 1}
	c.errmsg <- msg

	deadline := time.After(200 * time.Millisecond)
	for len(c.errmsg) > 0 {
		select {
		case <-deadline:
			is.Fail()
			return
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	close(c.done)
	close(c.pub)

	time.Sleep(1100 * time.Millisecond)
}

// no helpers
