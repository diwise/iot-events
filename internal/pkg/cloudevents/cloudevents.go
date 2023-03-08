package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/diwise/iot-events/internal/pkg/mediator"
)

type CloudEvents interface {
	Start(ctx context.Context)
}

type cloudeventsImpl struct {
	subscriber mediator.Subscriber
	mediator   mediator.Mediator
}

func (c *cloudeventsImpl) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.mediator.Unregister(c.subscriber)
			return
		case msg := <-c.subscriber.Mailbox():
			b, err := json.MarshalIndent(msg, "", "")
			if err != nil {
				fmt.Printf("error: %s", err.Error())
			}

			fmt.Printf("cloud event: %s", string(b))
		}
	}
}

func New(m mediator.Mediator) CloudEvents {
	s := &ceSubscriberImpl{
		id:      "cloud-events",
		notify:  make(chan mediator.Message),
		tenants: []string{"default"},
	}

	// TODO: read from file and register multiple (?) subscribers

	m.Register(s)

	return &cloudeventsImpl{
		subscriber: s,
		mediator:   m,
	}
}

type ceSubscriberImpl struct {
	id      string
	tenants []string
	notify  chan mediator.Message
}

func (s *ceSubscriberImpl) ID() string {
	return s.id
}
func (s *ceSubscriberImpl) Tenants() []string {
	return s.tenants
}
func (s *ceSubscriberImpl) Mailbox() chan mediator.Message {
	return s.notify
}
func (s *ceSubscriberImpl) Valid(m mediator.Message) bool {
	if m.Type() == "keep-alive" {
		return false
	}

	for _, t := range s.tenants {
		if t == m.Tenant() {
			return true
		}
	}
	return false
}
