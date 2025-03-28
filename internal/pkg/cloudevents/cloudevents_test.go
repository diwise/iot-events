package cloudevents

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/matryer/is"
)

func TestThatCloudEventIsSent(t *testing.T) {
	is := is.New(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := logging.NewContextWithLogger(context.Background(), logger)
	defer ctx.Done()

	resultChan := make(chan string)

	var err error

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		resultChan <- string(body)
	}))
	defer server.Close()

	r := strings.NewReader(strings.Replace(config, "<ENDPOINT URL>", server.URL, 1))
	cfg, err := LoadConfiguration(io.NopCloser(r))
	is.NoErr(err)

	m := mediator.New(logger)
	go m.Start(ctx)

	New(cfg, m).Start(context.Background())

	now := time.Now()

	ds := newDeviceStatusUpdated(now)

	m.Publish(ctx, mediator.NewMessage(ctx, "messageID", "device.statusUpdated", "default", ds))

	result := <-resultChan

	expected := fmt.Sprintf(`{"deviceID":"urn:ngsi-ld:Device:01","status":{"deviceID":"urn:ngsi-ld:Device:01","batteryLevel":0,"statusCode":0,"timestamp":"%s"},"tenant":"default","timestamp":"%s"}`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	is.Equal(expected, result)
}

func TestShouldNotBeSentIfTenantIsNotAllowed(t *testing.T) {
	is, subscriber := testSetup(t)

	m := mediator.MediatorMock{
		UnregisterFunc: func(subscriber mediator.Subscriber) {},
	}

	var calls int = 0

	go subscriber.run(context.Background(), &m, func(e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "secret", newDeviceStatusUpdated(time.Now()))
	subscriber.done <- true

	is.Equal(0, calls)
}

func TestShouldNotBeSentIfMessageBodyContainsNoDeviceID(t *testing.T) {
	is, subscriber := testSetup(t)

	m := mediator.MediatorMock{
		UnregisterFunc: func(subscriber mediator.Subscriber) {},
	}

	var calls int = 0

	go subscriber.run(context.Background(), &m, func(e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "default", []byte(`{ "devEUI":"id", "timestamp":"2023-03-15T10:25:30.936817754+01:00" }`))
	subscriber.done <- true

	is.Equal(0, calls)

}

func TestShouldNotBeSentIfIdPatternIsNotMatched(t *testing.T) {
	is, subscriber := testSetup(t)

	subscriber.idPatterns = append(subscriber.idPatterns, "^urn:ngsi-ld:Watermeter:.+")

	m := mediator.MediatorMock{
		UnregisterFunc: func(subscriber mediator.Subscriber) {},
	}

	var calls int = 0

	go subscriber.run(context.Background(), &m, func(e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "default", newDeviceStatusUpdated(time.Now()))
	subscriber.done <- true

	is.Equal(0, calls)
}

func TestOnlyAcceptIfValid(t *testing.T) {
	is, subscriber := testSetup(t)

	var calls int = 0

	go func() {
		for {
			select {
			case <-subscriber.inbox:
				calls++
			case <-subscriber.done:
				return
			}
		}
	}()

	subscriber.messageType = "device.statusUpdated"
	is.True(subscriber.Handle(mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "default", newDeviceStatusUpdated(time.Now()))))

	subscriber.messageType = "another.messageType"
	is.True(!subscriber.Handle(mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "default", newDeviceStatusUpdated(time.Now()))))

	subscriber.done <- true

	is.Equal(1, calls)
}

func TestShouldBeSent(t *testing.T) {
	is, subscriber := testSetup(t)

	subscriber.idPatterns = append(subscriber.idPatterns, "^urn:ngsi-ld:Device:.+")
	subscriber.tenants = append(subscriber.tenants, "anotherTenant")

	m := mediator.MediatorMock{
		UnregisterFunc: func(subscriber mediator.Subscriber) {},
	}

	var calls int = 0

	go subscriber.run(context.Background(), &m, func(e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "anotherTenant", newDeviceStatusUpdated(time.Now()))
	subscriber.done <- true

	is.Equal(1, calls)
}

func TestShouldBeSentForSecondPattern(t *testing.T) {
	is, subscriber := testSetup(t)

	subscriber.idPatterns = append(subscriber.idPatterns, "^urn:ngsi-ld:Device:.+")
	subscriber.idPatterns = append(subscriber.idPatterns, "^se:servanet:lora:msva:.+")
	subscriber.tenants = append(subscriber.tenants, "default")

	m := mediator.MediatorMock{
		UnregisterFunc: func(subscriber mediator.Subscriber) {},
	}

	var calls int = 0

	go subscriber.run(context.Background(), &m, func(e eventInfo) error {
		calls++
		return nil
	})

	msg := newDeviceStatusUpdated(time.Now())
	var dsu DeviceStatusUpdated
	json.Unmarshal(msg, &dsu)

	dsu.DeviceID = "se:servanet:lora:msva:05598380"
	b, _ := json.Marshal(dsu)

	subscriber.inbox <- mediator.NewMessage(context.Background(), "id", "device.statusUpdated", "default", b)
	subscriber.done <- true

	is.Equal(1, calls)
}

func TestNewWithEmptyConfig(t *testing.T) {
	is := is.New(t)
	m := mediator.MediatorMock{
		RegisterFunc: func(subscriber mediator.Subscriber) {},
	}

	New(LoadConfigurationFromFile(""), &m).Start(context.Background())

	is.Equal(0, len(m.RegisterCalls()))
}

func TestNewWithEmptyConfigFile(t *testing.T) {
	is := is.New(t)
	m := mediator.MediatorMock{
		RegisterFunc: func(subscriber mediator.Subscriber) {},
	}

	emptyConfigFile := ""
	configReader := strings.NewReader(emptyConfigFile)

	cfg, err := LoadConfiguration(io.NopCloser(configReader))
	is.NoErr(err)

	New(cfg, &m).Start(context.Background())

	is.Equal(0, len(m.RegisterCalls()))
}

func testSetup(t *testing.T) (*is.I, *ceSubscriberImpl) {
	is := is.New(t)

	subscriber := &ceSubscriberImpl{
		id:          "subscriber-01",
		done:        make(chan bool),
		inbox:       make(chan mediator.Message),
		endpoint:    "http://server.url",
		source:      "source",
		eventType:   "type",
		tenants:     []string{"default"},
		messageType: "device.statusUpdated",
		idPatterns:  []string{},
	}

	return is, subscriber
}

func newDeviceStatusUpdated(now time.Time) []byte {
	ds := DeviceStatusUpdated{
		DeviceID: "urn:ngsi-ld:Device:01",
		DeviceStatus: DeviceStatus{
			DeviceID:     "urn:ngsi-ld:Device:01",
			BatteryLevel: 0,
			Code:         0,
			Messages:     []string{},
			Timestamp:    now.Format(time.RFC3339Nano),
		},
		Tenant:    "default",
		Timestamp: now,
	}

	b, _ := json.Marshal(ds)
	return b
}

type DeviceStatusUpdated struct {
	DeviceID     string       `json:"deviceID"`
	DeviceStatus DeviceStatus `json:"status"`
	Tenant       string       `json:"tenant,omitempty"`
	Timestamp    time.Time    `json:"timestamp"`
}

type DeviceStatus struct {
	DeviceID     string   `json:"deviceID,omitempty"`
	BatteryLevel int      `json:"batteryLevel"`
	Code         int      `json:"statusCode"`
	Messages     []string `json:"statusMessages,omitempty"`
	Timestamp    string   `json:"timestamp"`
}
