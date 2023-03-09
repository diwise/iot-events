package mediator

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

type Message interface {
	ID() string
	Type() string
	Data() []byte
	Tenant() string
	Timestamp() time.Time
}

type messageImpl struct {
	Id_        string    `json:"id"`
	Name_      string    `json:"name"`
	Tenant_    string    `json:"tenant"`
	Data_      []byte    `json:"data"`
	Timestamp_ time.Time `json:"timestamp"`
}

func NewMessage(id, name, tenant string, data []byte) Message {
	return &messageImpl{
		Id_:        id,
		Name_:      name,
		Tenant_:    tenant,
		Data_:      data,
		Timestamp_: time.Now().UTC(),
	}
}

func (m *messageImpl) ID() string {
	return m.Id_
}

func (m *messageImpl) Type() string {
	return m.Name_
}

func (m *messageImpl) Data() []byte {
	return m.Data_
}

func (m *messageImpl) Tenant() string {
	return m.Tenant_
}

func (m *messageImpl) Timestamp() time.Time {
	return m.Timestamp_
}

type Subscriber interface {
	ID() string
	Tenants() []string
	Mailbox() chan Message
	AcceptIfValid(m Message)
}

type subscriberImpl struct {
	id      string
	tenants []string
	inbox   chan Message
}

func NewSubscriber(tenants []string) Subscriber {
	if len(tenants) == 0 {
		tenants = append(tenants, "default")
	}

	return &subscriberImpl{
		id:      fmt.Sprintf("%d", time.Now().Unix()),
		inbox:   make(chan Message),
		tenants: tenants,
	}
}

func (s *subscriberImpl) ID() string {
	return s.id
}
func (s *subscriberImpl) Tenants() []string {
	return s.tenants
}
func (s *subscriberImpl) Mailbox() chan Message {
	return s.inbox
}
func (s *subscriberImpl) AcceptIfValid(m Message) {
	for _, t := range s.tenants {
		if t == m.Tenant() {
			s.inbox <- m
		}
	}
}

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
	logger      zerolog.Logger
}

func New(logger zerolog.Logger) Mediator {
	return &mediatorImpl{
		inbox:       make(chan Message, 1),
		register:    make(chan Subscriber),
		unregister:  make(chan Subscriber),
		subscribers: map[string]Subscriber{},
		logger:      logger,
	}
}

func (m *mediatorImpl) Register(s Subscriber) {
	m.register <- s
}
func (m *mediatorImpl) Unregister(s Subscriber) {
	m.unregister <- s
}
func (m *mediatorImpl) Publish(msg Message) {
	m.logger.Debug().Msgf("publish message %s:%s", msg.Type(), msg.ID())
	m.inbox <- msg
}
func (m *mediatorImpl) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			m.logger.Debug().Msg("Done!")
			return
		case s := <-m.register:
			m.subscribers[s.ID()] = s
			m.logger.Debug().Msgf("register new subscriber %s. len: %d", s.ID(), len(m.subscribers))
		case s := <-m.unregister:
			delete(m.subscribers, s.ID())
			m.logger.Debug().Msgf("unregister subscriber %s. len: %d", s.ID(), len(m.subscribers))
		case msg := <-m.inbox:
			for _, s := range m.subscribers {
				s.AcceptIfValid(msg)
			}
		}
	}
}
