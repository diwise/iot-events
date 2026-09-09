package cloudevents

import (
	"io"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestThatConfigCouldBeRead(t *testing.T) {
	is := is.New(t)

	r := strings.NewReader(config)
	cfg, err := LoadConfiguration(io.NopCloser(r))

	is.NoErr(err)

	is.Equal(1, len(cfg.Subscribers))
	is.Equal("device.statusUpdated", cfg.Subscribers[0].Type)
	is.Equal("<ENDPOINT URL>", cfg.Subscribers[0].Endpoint)
	is.Equal("test", cfg.Subscribers[0].Tenants[1])
	is.Equal("^urn:ngsi-ld:Device:.+", cfg.Subscribers[0].Entities[0].IDPattern)
}

func TestThatConfigWithoutTenantCouldBeRead(t *testing.T) {
	is := is.New(t)

	r := strings.NewReader(configWithoutTenant)
	cfg, err := LoadConfiguration(io.NopCloser(r))

	is.NoErr(err)

	is.Equal(2, len(cfg.Subscribers))
	is.Equal("message.accepted", cfg.Subscribers[0].Type)
	is.Equal("function.updated", cfg.Subscribers[1].Type)
	is.Equal("http://srv/api/cloudevents", cfg.Subscribers[0].Endpoint)
	is.Equal(0, len(cfg.Subscribers[0].Tenants))
	is.Equal(0, len(cfg.Subscribers[0].Entities))
}

var config string = `
subscribers:
  - id: qalcosonic
    name: Qalcosonic W1 StatusCodes
    type: device.statusUpdated
    endpoint: <ENDPOINT URL>
    source: github.com/diwise/iot-device-mgmt
    eventType: diwise.statusmessage
    tenants:
      - default
      - test
    entities:
      - idPattern: ^urn:ngsi-ld:Device:.+
`

var configWithoutTenant string = `
subscribers:
  - id: api-rec-devices
    name: MessageAccepted
    type: message.accepted
    endpoint: http://srv/api/cloudevents
    source: github.com/diwise/iot-agent
    eventType: message.accepted
  - id: api-rec-functions
    name: FunctionUpdated
    type: function.updated
    endpoint: http://srv/api/cloudevents
    source: github.com/diwise/iot-core
    eventType: function.updated
`
