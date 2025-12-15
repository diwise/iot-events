package mqtt

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/iot-events/internal/pkg/devicemanagement"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

func TestValueWithPorts(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	result := make(chan TopicMessage, 1)

	p := &mqttPublisher{
		prefix:     "test/prefix",
		identifier: "deviceid",
		m:          &mediator.MediatorMock{},
		mqttClient: &ClientMock{
			PublishFunc: func(ctx context.Context, msg TopicMessage) error {
				result <- msg
				return nil
			},
		},
		dmc: &devicemanagement.ClientMock{
			GetDeviceFunc: func(ctx context.Context, deviceID string) (*devicemanagement.Device, error) {
				return &devicemanagement.Device{
					DeviceID: deviceID,
					SensorID: "sensor-123",
					Name:     "Temperature Sensor",
				}, nil
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
	is.Equal(topicMessage.TopicName(), "test/prefix/default/744691f2-3ad9-40b1-8879-58935bd2dadb/0/temperature")

	vm := valueMessage{}
	err := json.Unmarshal(topicMessage.Body(), &vm)
	is.NoErr(err)
	is.Equal(vm.Device.DeviceID, "744691f2-3ad9-40b1-8879-58935bd2dadb")
	is.Equal(vm.Type, "temperature")
	is.Equal(vm.Unit, "Cel")
	is.Equal(vm.Value, 21.5)
}

func TestValue(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	result := make(chan TopicMessage, 1)

	p := &mqttPublisher{
		prefix:     "sensors/prefix",
		identifier: "sensorid",
		m:          &mediator.MediatorMock{},
		mqttClient: &ClientMock{
			PublishFunc: func(ctx context.Context, msg TopicMessage) error {
				result <- msg
				return nil
			},
		},
		dmc: &devicemanagement.ClientMock{
			GetDeviceFunc: func(ctx context.Context, deviceID string) (*devicemanagement.Device, error) {
				return &devicemanagement.Device{
					DeviceID: deviceID,
					SensorID: "sensor-123",
					Name:     "Temperature Sensor",
				}, nil
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
	is.Equal(topicMessage.TopicName(), "sensors/prefix/default/sensor-123/temperature")

	vm := valueMessage{}
	err := json.Unmarshal(topicMessage.Body(), &vm)
	is.NoErr(err)
	is.Equal(vm.Device.DeviceID, "744691f2-3ad9-40b1-8879-58935bd2dadb")
	is.Equal(vm.Type, "temperature")
	is.Equal(vm.Unit, "Cel")
	is.Equal(vm.Value, 21.5)
}

func TestStatus(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	result := make(chan TopicMessage, 1)

	p := &mqttPublisher{
		prefix:     "sensors/prefix",
		identifier: "name",
		m:          &mediator.MediatorMock{},
		mqttClient: &ClientMock{
			PublishFunc: func(ctx context.Context, msg TopicMessage) error {
				result <- msg
				return nil
			},
		},
		dmc: &devicemanagement.ClientMock{
			GetDeviceFunc: func(ctx context.Context, deviceID string) (*devicemanagement.Device, error) {
				return &devicemanagement.Device{
					DeviceID: deviceID,
					SensorID: "sensor-123",
					Name:     "Temperature Sensor",
				}, nil
			},
		},
	}
	sub := newSubscriber("device-status", p.newStatusMessageHandler)
	go sub.run(ctx)

	tenant := "default"

	timestamp := time.Now()
	tm := struct {
		DeviceID        string    `json:"deviceID"`
		RSSI            *float64  `json:"rssi,omitempty"`
		LoRaSNR         *float64  `json:"loRaSNR,omitempty"`
		Frequency       *int64    `json:"frequency,omitempty"`
		SpreadingFactor *float64  `json:"spreadingFactor,omitempty"`
		DR              *int      `json:"dr,omitempty"`
		Timestamp       time.Time `json:"timestamp"`
	}{
		DeviceID:        "744691f2-3ad9-40b1-8879-58935bd2dadb",
		RSSI:            floatPtr(-70.5),
		LoRaSNR:         floatPtr(7.5),
		Frequency:       int64Ptr(868100000),
		SpreadingFactor: floatPtr(7),
		DR:              intPtr(5),
		Timestamp:       timestamp,
	}

	b, _ := json.Marshal(tm)
	sub.Handle(mediator.NewMessage(ctx, "1", "device-status", tenant, b))

	topicMessage := <-result
	is.Equal(topicMessage.TopicName(), "sensors/prefix/default/Temperature-Sensor/status")

	sm := statusMessage{}
	err := json.Unmarshal(topicMessage.Body(), &sm)
	is.NoErr(err)
	is.Equal(sm.Device.DeviceID, "744691f2-3ad9-40b1-8879-58935bd2dadb")
	is.Equal(sm.RSSI, floatPtr(-70.5))
	is.Equal(sm.LoRaSNR, floatPtr(7.5))
	is.Equal(sm.Frequency, int64Ptr(868100000))
	is.Equal(sm.SpreadingFactor, floatPtr(7))
	is.Equal(sm.DR, intPtr(5))
	is.True(sm.Timestamp.Equal(timestamp))
}

func floatPtr(f float64) *float64 {
	return &f
}

func int64Ptr(i int64) *int64 {
	return &i
}

func intPtr(i int) *int {
	return &i
}
