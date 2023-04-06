package handlers

import (
	"testing"

	"github.com/matryer/is"
)

func TestThatChannelIsExtractedFromRoutingKey(t *testing.T) {
	is := is.New(t)

	routingKey := "device.statusUpdated"
	channel := getChannelName(routingKey)

	is.Equal("device", channel)
}
