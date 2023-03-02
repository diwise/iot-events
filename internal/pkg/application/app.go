package application

import (
	"encoding/json"
	"time"
)

type App interface {
	Listen()
	GetBroker() *Broker
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
		a.broker.Notifier <- NewMessage("", "keep-alive", nil)
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
				client.Notify <- event
			}
		}
	}
}

func (a *app) Add(client Client){
	a.broker.AddClient <- client
}

func (a *app) Close(client Client) {
	a.broker.ClosingClients <- client
}

func (a *app) Notify(message Message) {
	a.broker.Notifier <- message
}

func (a *app) GetBroker() *Broker {
	return a.broker
}

type Client struct {
	ID      string
	Tenants []string
	Notify  chan Message
}

type message struct {
	ID    string
	Event string
	Data  any
}

type Message interface {
	Format() string
}

func NewMessage(id, event string, data any) Message {
	return &message{
		ID:    id,
		Event: event,
		Data:  data,
	}
}

func (m *message) Format() string {
	msg := ""

	if m.ID != "" {
		msg = msg + "id: " + m.ID + "\n"
	}
	if m.Event != "" {
		msg = msg + "event: " + m.Event + "\n"
	}
	if m.Data != nil {
		if b, err := json.Marshal(m.Data); err == nil {
			msg = msg + "data: " + string(b) + "\n"
		}
	}

	msg = msg + "\n"

	return msg
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
