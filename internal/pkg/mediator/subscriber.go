package mediator

import (
	"slices"

	"github.com/google/uuid"
)

type Subscriber interface {
	ID() string
	Mailbox() chan Message
	Handle(m Message) bool
}

type subscriberImpl struct {
	id             string
	allowedTenants []string
	inbox          chan Message
}

func NewSubscriber(tenants []string) Subscriber {
	if len(tenants) == 0 {
		tenants = append(tenants, "default")
	}

	id := uuid.New().String()

	return &subscriberImpl{
		id:             id,
		inbox:          make(chan Message),
		allowedTenants: tenants,
	}
}

func (s *subscriberImpl) ID() string {
	return s.id
}

func (s *subscriberImpl) Mailbox() chan Message {
	return s.inbox
}
func (s *subscriberImpl) Handle(m Message) bool {
	if slices.Contains(s.allowedTenants, m.Tenant()) {
		s.inbox <- m
		return true
	}
	return false
}
