package mediator

import (
	"context"
	"fmt"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

//go:generate moq -rm -out mediator_mock.go . Mediator

type Mediator interface {
	Register(subscriber Subscriber)
	Unregister(subscriber Subscriber)
	Publish(message Message)
	Start(ctx context.Context)
}

type mediatorImpl struct {
	inbox       chan Message
	register    chan Subscriber
	unregister  chan Subscriber
	subscribers map[string]Subscriber
}

func New(ctx context.Context) Mediator {
	return &mediatorImpl{
		inbox:       make(chan Message, 1),
		register:    make(chan Subscriber),
		unregister:  make(chan Subscriber),
		subscribers: map[string]Subscriber{},
	}
}

func (m *mediatorImpl) Register(s Subscriber) {
	m.register <- s
}
func (m *mediatorImpl) Unregister(s Subscriber) {
	m.unregister <- s
}

func (m *mediatorImpl) Publish(msg Message) {
	log := logging.GetFromContext(msg.Context())
	log.Debug(fmt.Sprintf("publishing message %s of type %s", msg.ID(), msg.Type()))

	m.inbox <- msg
}

func (m *mediatorImpl) Start(ctx context.Context) {
	log := logging.GetFromContext(ctx)
	log.Debug(fmt.Sprintf("starting mediator with %d subscribers", len(m.subscribers)))

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug("context is done")
				return

			case s := <-m.register:
				m.subscribers[s.ID()] = s
				log.Debug("register new subscriber", "subscriber_id", s.ID(), "total", len(m.subscribers))

			case s := <-m.unregister:
				delete(m.subscribers, s.ID())
				log.Debug("unregister subscriber", "subscriber_id", s.ID(), "total", len(m.subscribers))

			case msg := <-m.inbox:
				for _, s := range m.subscribers {
					s.Handle(msg)
				}
			}
		}
	}()
}
