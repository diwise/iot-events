package mediator

import (
	"context"
	"time"
)

type Message interface {
	ID() string
	Type() string
	Data() []byte
	Tenant() string
	Retry() int
	Timestamp() time.Time
	Context() context.Context
}

type messageImpl struct {
	Id_        string          `json:"id"`
	Name_      string          `json:"name"`
	Tenant_    string          `json:"tenant"`
	Data_      []byte          `json:"data"`
	Retry_     int             `json:"retry"`
	Timestamp_ time.Time       `json:"timestamp"`
	ctx        context.Context `json:"-"`
}

func NewMessage(ctx context.Context, id, name, tenant string, data []byte) Message {
	return &messageImpl{
		Id_:        id,
		Name_:      name,
		Tenant_:    tenant,
		Data_:      data,
		Retry_:     0,
		Timestamp_: time.Now().UTC(),
		ctx:        ctx,
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

func (m *messageImpl) Retry() int {
	return m.Retry_
}

func (m *messageImpl) Timestamp() time.Time {
	return m.Timestamp_
}

func (m *messageImpl) Context() context.Context {
	return m.ctx
}
