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
	ctx := logging.NewContextWithLogger(t.Context(), logger)
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

	m := mediator.New(ctx)
	m.Start(ctx)

	cloudEvents := New(cfg, m)
	cloudEvents.Start(ctx)

	now := time.Now()

	m.Publish(mediator.NewMessage(ctx, "messageID", "device.statusUpdated", "default", newDeviceStatusUpdated(now)))

	result := <-resultChan

	expected := fmt.Sprintf(`{"deviceID":"urn:ngsi-ld:Device:01","status":{"deviceID":"urn:ngsi-ld:Device:01","batteryLevel":0,"statusCode":0,"timestamp":"%s"},"tenant":"default","timestamp":"%s"}`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	is.Equal(expected, result)
}

func TestShouldNotBeSentIfTenantIsNotAllowed(t *testing.T) {
	is, subscriber := testSetup(t)

	var calls int = 0
	ctx := t.Context()

	go subscriber.run(ctx, func(ctx context.Context, e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(ctx, "id", "device.statusUpdated", "secret", newDeviceStatusUpdated(time.Now()))

	is.Equal(0, calls)
	time.Sleep(1 * time.Second)
}

func TestShouldNotBeSentIfMessageBodyContainsNoDeviceID(t *testing.T) {
	is, subscriber := testSetup(t)

	var calls int = 0
	ctx := t.Context()

	go subscriber.run(ctx, func(ctx context.Context, e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(ctx, "id", "device.statusUpdated", "default", []byte(`{ "devEUI":"id", "timestamp":"2023-03-15T10:25:30.936817754+01:00" }`))

	is.Equal(0, calls)
	time.Sleep(1 * time.Second)
}

func TestShouldNotBeSentIfIdPatternIsNotMatched(t *testing.T) {
	is, subscriber := testSetup(t)
	ctx := t.Context()

	subscriber.idPatterns = append(subscriber.idPatterns, "^urn:ngsi-ld:Watermeter:.+")

	var calls int = 0

	go subscriber.run(ctx, func(ctx context.Context, e eventInfo) error {
		calls++
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(ctx, "id", "device.statusUpdated", "default", newDeviceStatusUpdated(time.Now()))

	is.Equal(0, calls)
	time.Sleep(1 * time.Second)
}

func TestShouldBeSent(t *testing.T) {
	is, subscriber := testSetup(t)

	resultChan := make(chan int)

	ctx := context.Background()
	defer ctx.Done()

	subscriber.idPatterns = append(subscriber.idPatterns, "^urn:ngsi-ld:Device:.+")
	subscriber.tenants = append(subscriber.tenants, "anotherTenant")

	var calls int = 0

	go subscriber.run(ctx, func(ctx context.Context, e eventInfo) error {
		calls++
		resultChan <- calls
		return nil
	})

	subscriber.inbox <- mediator.NewMessage(ctx, "id", "device.statusUpdated", "anotherTenant", newDeviceStatusUpdated(time.Now()))

	is.Equal(1, <-resultChan)
}

func testSetup(t *testing.T) (*is.I, *cloudEventSubscriber) {
	is := is.New(t)

	subscriber := &cloudEventSubscriber{
		id:          "subscriber-01",
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
