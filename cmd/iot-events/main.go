package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/diwise/iot-events/internal/pkg/application"
	"github.com/diwise/iot-events/internal/pkg/cloudevents"
	"github.com/diwise/iot-events/internal/pkg/measurements"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

func defaultFlags() flagMap {
	return flagMap{
		listenAddress: "0.0.0.0",
		servicePort:   "8080",
		controlPort:   "8000",

		cloudeventsFile: "/opt/diwise/config/cloudevents.yaml",
		policiesFile:    "/opt/diwise/config/authz.rego",
		metadataFile:    "/opt/diwise/config/metadata.csv",

		messengerTopic: "#",

		dbHost:     "",
		dbUser:     "",
		dbPassword: "",
		dbPort:     "5432",
		dbName:     "diwise",
		dbSSLMode:  "disable",
	}
}

const serviceName string = "iot-events"

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	cf, err := os.Open(flags[cloudeventsFile])
	exitIf(err, logger, "unable to open cloudevents config file")

	cloudeventsConfig, err := cloudevents.LoadConfiguration(cf)
	exitIf(err, logger, "unable to load cloudevents config")

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	var mf io.ReadCloser
	mf, err = os.Open(flags[metadataFile])
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Warn("metadata file does not exist, proceeding without initial metadata", "file", flags[metadataFile])
			mf = io.NopCloser(strings.NewReader(""))
		} else {
			exitIf(err, logger, "unable to open metadata config file")
		}
	}

	messengerConfig := messaging.LoadConfiguration(ctx, serviceName, logger)
	storageConfig := storage.NewConfig(flags[dbHost], flags[dbPort], flags[dbName], flags[dbUser], flags[dbPassword], flags[dbSSLMode])

	cfg := &appConfig{
		storageConfig:     storageConfig,
		messengerConfig:   messengerConfig,
		cloudeventsConfig: cloudeventsConfig,
	}

	runner, _ := initialize(ctx, flags, cfg, policies, mf)

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile io.ReadCloser, metadataFile io.ReadCloser) (servicerunner.Runner[appConfig], error) {
	defer policiesFile.Close()
	defer metadataFile.Close()

	var err error

	var messenger messaging.MsgContext
	var s storage.Storage
	var ce cloudevents.CloudEvents
	var m mediator.Mediator

	probes := map[string]k8shandlers.ServiceProber{
		"rabbitmq":  func(context.Context) (string, error) { return "ok", nil },
		"timescale": func(context.Context) (string, error) { return "ok", nil },
	}

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]),
			muxinit(func(ctx context.Context, identifier string, port string, svcCfg *appConfig, handler *http.ServeMux) error {
				api.RegisterHandlers(ctx, serviceName, handler, m, s, policiesFile)
				return nil
			}),
		),
		oninit(func(ctx context.Context, cfg *appConfig) error {
			m = mediator.New(ctx)

			s, err = storage.New(ctx, cfg.storageConfig)
			if err != nil {
				return fmt.Errorf("could not create storage %w", err)
			}

			messenger, err = messaging.Initialize(ctx, cfg.messengerConfig)
			if err != nil {
				return fmt.Errorf("could not initialize messenger %w", err)
			}

			ce = cloudevents.New(cfg.cloudeventsConfig, m)

			metadata, err := measurements.LoadMetadata(ctx, metadataFile)
			if err != nil {
				return fmt.Errorf("failed to load metadata: %w", err)
			}

			err = s.SeedMetadata(ctx, metadata)
			if err != nil {
				return fmt.Errorf("could not seed metadata %w", err)
			}

			return nil
		}),
		onstarting(func(ctx context.Context, svcCfg *appConfig) (err error) {

			m.Start(ctx)
			ce.Start(ctx)
			messenger.Start()

			messenger.RegisterTopicMessageHandler(flags[messengerTopic], application.NewMessageHandler(m))
			messenger.RegisterTopicMessageHandler("message.accepted", measurements.NewMessageAcceptedHandler(s))

			return nil
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			messenger.Close()

			return nil
		}),
	)

	return runner, nil
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {

	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])
	flags[controlPort] = envOrDef(ctx, "CONTROL_PORT", flags[controlPort])

	flags[messengerTopic] = envOrDef(ctx, "RABBITMQ_TOPIC", flags[messengerTopic])

	flags[dbHost] = envOrDef(ctx, "POSTGRES_HOST", flags[dbHost])
	flags[dbPort] = envOrDef(ctx, "POSTGRES_PORT", flags[dbPort])
	flags[dbName] = envOrDef(ctx, "POSTGRES_DBNAME", flags[dbName])
	flags[dbUser] = envOrDef(ctx, "POSTGRES_USER", flags[dbUser])
	flags[dbPassword] = envOrDef(ctx, "POSTGRES_PASSWORD", flags[dbPassword])
	flags[dbSSLMode] = envOrDef(ctx, "POSTGRES_SSLMODE", flags[dbSSLMode])

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	// Allow command line arguments to override defaults and environment variables
	flag.Func("cloudevents", "configuration file for cloud events", apply(cloudeventsFile))
	flag.Func("policies", "an authorization policy file", apply(policiesFile))
	flag.Func("metadata", "a CSV file with initial metadata", apply(metadataFile))

	flag.Parse()

	return ctx, flags
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		os.Exit(1)
	}
}
