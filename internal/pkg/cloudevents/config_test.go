package cloudevents

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestThatConfigCouldBeRead(t *testing.T) {
	is := is.New(t)

	r := strings.NewReader(config)
	cfg, err := LoadConfiguration(r)

	is.NoErr(err)

	is.Equal(1, len(cfg.Subscribers))
	is.Equal("device.statusUpdated", cfg.Subscribers[0].Type)
	is.Equal("<ENDPOINT URL>", cfg.Subscribers[0].Endpoint)
	is.Equal("test", cfg.Subscribers[0].Tenants[1])
	is.Equal("^urn:ngsi-ld:Device:.+", cfg.Subscribers[0].Entities[0].IDPattern)
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
