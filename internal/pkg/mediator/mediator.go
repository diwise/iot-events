package mediator

import (
	"context"
	"sync"

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
	subMu       sync.RWMutex
	startOnce   sync.Once
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
	m.inbox <- msg
}

func (m *mediatorImpl) Start(ctx context.Context) {
	m.startOnce.Do(func() {

		log := logging.GetFromContext(ctx)

		go func() {
			for {
				select {
				case <-ctx.Done():
					return

				case s := <-m.register:
					m.subMu.Lock()
					m.subscribers[s.ID()] = s
					total := len(m.subscribers)
					m.subMu.Unlock()
					log.Debug("register new subscriber", "subscriber_id", s.ID(), "total", total)

				case s := <-m.unregister:
					m.subMu.Lock()
					delete(m.subscribers, s.ID())
					total := len(m.subscribers)
					m.subMu.Unlock()
					log.Debug("unregister subscriber", "subscriber_id", s.ID(), "total", total)

				case msg := <-m.inbox:
					log := logging.GetFromContext(msg.Context())
					log.Debug("publishing message to subscribers", "message_id", msg.ID())

					m.subMu.RLock()
					subs := make([]Subscriber, 0, len(m.subscribers))
					for _, sub := range m.subscribers {
						subs = append(subs, sub)
					}
					m.subMu.RUnlock()

					for _, sub := range subs {
						go func(handler Subscriber) {
							handler.Handle(msg)
						}(sub)
					}
				}
			}
		}()
	})
}
