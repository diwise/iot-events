package application

import (
	"encoding/base64"
	"fmt"
	"time"
)

type App interface {
	Listen()
	Add(client Client)
	Close(client Client)
	Notify(message Message)
}

type app struct {
	broker *Broker
}

func New(b *Broker) App {
	a := &app{
		broker: b,
	}

	go a.Listen()
	go a.KeepAlive()

	return a
}

func (a *app) KeepAlive() {
	for {
		a.broker.Notifier <- NewMessage("", "keep-alive", "default", nil)
		time.Sleep(10 * time.Second)
	}
}

func (a *app) Listen() {
	for {
		select {
		case client := <-a.broker.AddClient:
			a.broker.Clients[client.ID] = client
		case client := <-a.broker.ClosingClients:
			delete(a.broker.Clients, client.ID)
		case event := <-a.broker.Notifier:
			for _, client := range a.broker.Clients {
				if client.Allow(event.Tenant()) {
					client.Notify <- event
				}
			}
		}
	}
}

func (a *app) Add(client Client) {
	a.broker.AddClient <- client
}

func (a *app) Close(client Client) {
	a.broker.ClosingClients <- client
}

func (a *app) Notify(message Message) {
	a.broker.Notifier <- message
}

type Client struct {
	ID      string
	Tenants []string
	Notify  chan Message
}

func NewClient(tenants []string) Client {
	if len(tenants) == 0 {
		tenants = append(tenants, "default")
	}

	return Client{
		ID:      fmt.Sprintf("%d", time.Now().Unix()),
		Notify:  make(chan Message),
		Tenants: tenants,
	}
}

func (c *Client) Allow(tenant string) bool {
	for _, t := range c.Tenants {
		if t == tenant {
			return true
		}
	}
	return false
}

type message struct {
	id        string
	eventName string
	tenant    string
	data      []byte
}

type Message interface {
	Format() string
	Tenant() string
}

func NewMessage(id, event, tenant string, data []byte) Message {
	return &message{
		id:        id,
		eventName: event,
		data:      data,
		tenant:    tenant,
	}
}

func (m *message) Format() string {
	msg := ""

	if m.id != "" {
		msg = msg + "id: " + m.id + "\n"
	}
	if m.eventName != "" {
		msg = msg + "event: " + m.eventName + "\n"
	}
	if m.data != nil {
		b64 := base64.StdEncoding.EncodeToString(m.data)
		msg = msg + "data: " + b64 + "\n"
	}

	msg = msg + "\n"

	return msg
}

func (m *message) Tenant() string {
	return m.tenant
}

type Broker struct {
	Notifier       chan Message
	AddClient      chan Client
	ClosingClients chan Client
	Clients        map[string]Client
}

func NewBroker() *Broker {
	return &Broker{
		Notifier:       make(chan Message, 1),
		AddClient:      make(chan Client),
		ClosingClients: make(chan Client),
		Clients:        make(map[string]Client),
	}
}
