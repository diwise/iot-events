package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/diwise/iot-events/internal/pkg/cloudevents"
	"github.com/diwise/iot-events/internal/pkg/handlers"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"
)

var serviceName string = "iot-events"

func main() {
	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	var cloudeventsConfigFilePath string
	var opaFilePath string

	flag.StringVar(&cloudeventsConfigFilePath, "cloudevents", "/opt/diwise/config/cloudevents.yaml", "Configuration file for cloudevents")
	flag.StringVar(&opaFilePath, "policies", "/opt/diwise/config/authz.rego", "An authorization policy file")
	flag.Parse()

	ctx = logging.NewContextWithLogger(ctx, logger)

	mediator := mediator.New(logger)
	go mediator.Start(ctx)

	api := func() chi.Router {
		policies, err := os.Open(opaFilePath)
		if err != nil {
			logger.Fatal().Err(err).Msg("unable to open opa policy file")
		}
		defer policies.Close()

		return api.New(serviceName, mediator, policies, logger)
	}()

	apiPort := fmt.Sprintf(":%s", env.GetVariableOrDefault(logger, "SERVICE_PORT", "8080"))

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	topic := env.GetVariableOrDefault(logger, "RABBITMQ_TOPIC", "#")

	messenger.RegisterTopicMessageHandler(topic, handlers.NewTopicMessageHandler(messenger, mediator))

	cloudevents.New(cloudevents.LoadConfigurationFromFile(cloudeventsConfigFilePath), mediator, logger)

	http.ListenAndServe(apiPort, api)
}
