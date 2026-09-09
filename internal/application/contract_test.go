package application

import (
	"context"
	"log/slog"
	"testing"

	"github.com/diwise/iot-events/internal/infrastructure/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

func contractIncomingMessage(body, topic, contentType string) *messaging.IncomingTopicMessageMock {
	return &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return topic },
		ContentTypeFunc: func() string { return contentType },
	}
}

// HARM-003: locks the tenant extraction contract of the generic topic
// handler. Tenant in the SenML pack takes precedence over the top-level
// tenant field; the topic name and original body pass through untouched.
func TestTenantFromSenMLPackTakesPrecedence(t *testing.T) {
	is := is.New(t)

	var published []mediator.Message
	m := &mediator.MediatorMock{
		PublishFunc: func(message mediator.Message) { published = append(published, message) },
	}

	body := `{"tenant":"top-level","pack":[{"n":"tenant","vs":"from-pack"}]}`
	NewMessageHandler(m)(context.Background(), contractIncomingMessage(body, "message.accepted", "application/json"), slog.Default())

	is.Equal(len(published), 1)
	is.Equal(published[0].Tenant(), "from-pack")
	is.Equal(published[0].Type(), "message.accepted")
	is.Equal(string(published[0].Data()), body)
}

// HARM-003: locks the top-level tenant fallback used for topics whose
// payload carries no SenML pack.
func TestTopLevelTenantFallback(t *testing.T) {
	is := is.New(t)

	var published []mediator.Message
	m := &mediator.MediatorMock{
		PublishFunc: func(message mediator.Message) { published = append(published, message) },
	}

	body := `{"tenant":"fallback"}`
	NewMessageHandler(m)(context.Background(), contractIncomingMessage(body, "device-status", "application/json"), slog.Default())

	is.Equal(len(published), 1)
	is.Equal(published[0].Tenant(), "fallback")
	is.Equal(published[0].Type(), "device-status")
}

// HARM-003: locks the drop behavior for messages that carry no tenant
// in either position.
func TestMessageWithoutTenantIsDropped(t *testing.T) {
	is := is.New(t)

	var published []mediator.Message
	m := &mediator.MediatorMock{
		PublishFunc: func(message mediator.Message) { published = append(published, message) },
	}

	NewMessageHandler(m)(context.Background(), contractIncomingMessage(`{"pack":[]}`, "message.accepted", "application/json"), slog.Default())

	is.Equal(len(published), 0)
}
