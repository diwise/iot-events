package mediator

import (
	"fmt"
	"time"
)

type Message interface {
	ID() string
	Type() string
	Data() []byte
	Tenant() string
}

type messageImpl struct {
	Id_     string `json:"id"`
	Name_   string `json:"name"`
	Tenant_ string `json:"tenant"`
	Data_   []byte `json:"data"`
}

func NewMessage(id, name, tenant string, data []byte) Message {
	return &messageImpl{
		Id_:     id,
		Name_:   name,
		Tenant_: tenant,
		Data_:   data,
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

type Subscriber interface {
	ID() string
	Tenants() []string
	Mailbox() chan Message
	Allowed(m Message) bool
}

type subscriberImpl struct {
	id      string
	tenants []string
	notify  chan Message
}

func NewSubscriber(tenants []string) Subscriber {
	if len(tenants) == 0 {
		tenants = append(tenants, "default")
	}

	return &subscriberImpl{
		id:      fmt.Sprintf("%d", time.Now().Unix()),
		notify:  make(chan Message),
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
	return s.notify
}
func (s *subscriberImpl) Allowed(m Message) bool {
	for _, t := range s.tenants {
		if t == m.Tenant() {
			return true
		}
	}
	return false
}

type Mediator interface {
	Register(subscriber Subscriber)
	Unregister(subscriber Subscriber)
	Publish(message Message)
	Start()
}

type mediatorImpl struct {
	inbox       chan Message
	register    chan Subscriber
	unregister  chan Subscriber
	subscribers map[string]Subscriber
}

func New() Mediator {
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
func (m *mediatorImpl) Start() {
	for {
		select {
		case s := <-m.register:
			m.subscribers[s.ID()] = s
		case s := <-m.unregister:
			delete(m.subscribers, s.ID())
		case msg := <-m.inbox:
			for _, s := range m.subscribers {
				if s.Allowed(msg) {
					s.Mailbox() <- msg
				}
			}
		}
	}
}
