package mqtt

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

func TestValueWithPorts(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	result := make(chan TopicMessage, 1)

	p := &mqttPublisher{
		prefix: "test/prefix",
		m:      &mediator.MediatorMock{},
		c: &ClientMock{
			PublishFunc: func(ctx context.Context, msg TopicMessage) error {
				result <- msg
				return nil
			},
		},
	}
	sub := newSubscriber("message.accepted", p.newMessageAcceptedHandler)
	go sub.run(ctx)

	tenant := "default"
	v := 21.5

	pack := &senml.Pack{
		senml.Record{
			BaseName:    "744691f2-3ad9-40b1-8879-58935bd2dadb/0/3303/",
			BaseTime:    float64(time.Now().Unix()),
			Name:        "0",
			StringValue: "urn:oma:lwm2m:ext:3303",
		},
		senml.Record{
			Name:  "5700",
			Value: &v,
			Unit:  senml.UnitCelsius,
		},
		senml.Record{
			Name:        "tenant",
			StringValue: tenant,
		},
	}

	m := struct {
		Tenant *string     `json:"tenant,omitempty"`
		Pack   *senml.Pack `json:"pack,omitempty"`
	}{
		Tenant: &tenant,
		Pack:   pack,
	}

	b, _ := json.Marshal(m)
	sub.Handle(mediator.NewMessage(ctx, "1", "message.accepted", tenant, b))

	topicMessage := <-result
	is.Equal(topicMessage.TopicName(), "devices/test/prefix/default/744691f2-3ad9-40b1-8879-58935bd2dadb/0/temperature")

	vm := valueMessage{}
	err := json.Unmarshal(topicMessage.Body(), &vm)
	is.NoErr(err)
	is.Equal(vm.DeviceID, "744691f2-3ad9-40b1-8879-58935bd2dadb")
	is.Equal(vm.Type, "temperature")
	is.Equal(vm.Unit, "Cel")
	is.Equal(vm.Value, 21.5)
}

func TestValue(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	result := make(chan TopicMessage, 1)

	p := &mqttPublisher{
		prefix: "test/prefix",
		m:      &mediator.MediatorMock{},
		c: &ClientMock{
			PublishFunc: func(ctx context.Context, msg TopicMessage) error {
				result <- msg
				return nil
			},
		},
	}
	sub := newSubscriber("message.accepted", p.newMessageAcceptedHandler)
	go sub.run(ctx)

	tenant := "default"
	v := 21.5

	pack := &senml.Pack{
		senml.Record{
			BaseName:    "744691f2-3ad9-40b1-8879-58935bd2dadb/3303/",
			BaseTime:    float64(time.Now().Unix()),
			Name:        "0",
			StringValue: "urn:oma:lwm2m:ext:3303",
		},
		senml.Record{
			Name:  "5700",
			Value: &v,
			Unit:  senml.UnitCelsius,
		},
		senml.Record{
			Name:        "tenant",
			StringValue: tenant,
		},
	}

	m := struct {
		Tenant *string     `json:"tenant,omitempty"`
		Pack   *senml.Pack `json:"pack,omitempty"`
	}{
		Tenant: &tenant,
		Pack:   pack,
	}

	b, _ := json.Marshal(m)
	sub.Handle(mediator.NewMessage(ctx, "1", "message.accepted", tenant, b))

	topicMessage := <-result
	is.Equal(topicMessage.TopicName(), "devices/test/prefix/default/744691f2-3ad9-40b1-8879-58935bd2dadb/temperature")

	vm := valueMessage{}
	err := json.Unmarshal(topicMessage.Body(), &vm)
	is.NoErr(err)
	is.Equal(vm.DeviceID, "744691f2-3ad9-40b1-8879-58935bd2dadb")
	is.Equal(vm.Type, "temperature")
	is.Equal(vm.Unit, "Cel")
	is.Equal(vm.Value, 21.5)
}