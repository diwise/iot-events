package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/diwise/iot-events/internal/pkg/application"
	"github.com/diwise/iot-events/internal/pkg/handlers"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
)

var serviceName string = "iot-events"

func main() {
	serviceVersion := buildinfo.SourceVersion()
	_, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	broker := application.NewBroker()
	app := application.New(broker)
	api := api.New(serviceName, app)

	apiPort := fmt.Sprintf(":%s", env.GetVariableOrDefault(logger, "SERVICE_PORT", "8080"))

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	messenger.RegisterTopicMessageHandler("*", handlers.NewTopicMessageHandler(messenger, app))

	http.ListenAndServe(apiPort, api)
}
