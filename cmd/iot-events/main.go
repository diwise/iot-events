package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"

	"github.com/diwise/iot-events/internal/pkg/cloudevents"
	"github.com/diwise/iot-events/internal/pkg/handlers"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

var serviceName string = "iot-events"

func main() {
	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	var notificationConfigPath string
	flag.StringVar(&notificationConfigPath, "notifications", "/opt/diwise/config/cloudevents.yaml", "Configuration file for notifications")
	flag.Parse()

	ctx = logging.NewContextWithLogger(ctx, logger)

	mediator := mediator.New(logger)
	go mediator.Start(ctx)

	api := api.New(serviceName, mediator)
	apiPort := fmt.Sprintf(":%s", env.GetVariableOrDefault(logger, "SERVICE_PORT", "8080"))

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	topic := env.GetVariableOrDefault(logger, "RABBITMQ_TOPIC", "#")

	messenger.RegisterTopicMessageHandler(topic, handlers.NewTopicMessageHandler(messenger, mediator))

	cloudevents.New(cloudevents.LoadConfigurationFromFile(notificationConfigPath), mediator, logger)

	http.ListenAndServe(apiPort, api)
}
