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
	messagecollector "github.com/diwise/iot-events/internal/pkg/msgcollector"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/iot-events/internal/pkg/storage"
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

	mediator := mediator.New(logger)
	go mediator.Start(ctx)

	config := messaging.LoadConfiguration(ctx, serviceName, logger)
	messenger, err := messaging.Initialize(ctx, config)
	if err != nil {
		fatal(ctx, "failed to init messenger", err)
	}
	messenger.Start()

	topic := env.GetVariableOrDefault(ctx, "RABBITMQ_TOPIC", "#")

	messenger.RegisterTopicMessageHandler(topic, handlers.NewTopicMessageHandler(messenger, mediator))

	storage, err := storage.New(ctx, storage.LoadConfiguration(ctx))
	if err != nil {
		fatal(ctx, "faild to connect to storage", err)
	}

	ce := cloudevents.New(cloudevents.LoadConfigurationFromFile(cloudeventsConfigFilePath), mediator)
	ce.Start(ctx)

	mc := messagecollector.New(mediator, storage)
	mc.Start(ctx)

	api, err := func() (chi.Router, error) {
		policies, err := os.Open(opaFilePath)
		if err != nil {
			return nil, fmt.Errorf("unable to open opa policy file: %w", err)
		}
		defer policies.Close()

		return api.New(ctx, serviceName, mediator, storage, policies)
	}()
	if err != nil {
		panic(err)
	}

	apiPort := fmt.Sprintf(":%s", env.GetVariableOrDefault(ctx, "SERVICE_PORT", "8080"))

	http.ListenAndServe(apiPort, api)
}

func fatal(ctx context.Context, msg string, err error) {
	logger := logging.GetFromContext(ctx)
	logger.Error(msg, "err", err.Error())
	os.Exit(1)
}
