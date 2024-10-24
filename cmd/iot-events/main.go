package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion)
	defer cleanup()

	messenger, err := messaging.Initialize(
		ctx, messaging.LoadConfiguration(ctx, serviceName, logger),
	)
	exitIf(err, logger, "failed to init messenger")

	storage, err := storage.New(ctx, storage.NewConfig(
		flags[dbHost], flags[dbPort], flags[dbName],
		flags[dbUser], flags[dbPassword], flags[dbSSLMode]),
	)
	exitIf(err, logger, "failed to connect to storage: %s", err.Error())

	cloudEventsConfig, err := os.Open(flags[cloudeventsFile])
	exitIf(err, logger, "unable to open cloudevents config file")

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	cfg := &appConfig{
		mediator:  mediator.New(logger),
		messenger: messenger,
		storage:   storage,
	}

	runner, _ := initialize(ctx, flags, cfg, policies, cloudEventsConfig)

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policies io.ReadCloser, cloudconfig io.ReadCloser) (servicerunner.Runner[appConfig], error) {
	defer cloudconfig.Close()
	defer policies.Close()

	cecfg, err := cloudevents.LoadConfiguration(cloudconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load cloudevents config: %s", err.Error())
	}

	cfg.ce = cloudevents.New(cecfg, cfg.mediator)
	cfg.mc = messagecollector.New(cfg.mediator, cfg.storage)

	cfg.messenger.RegisterTopicMessageHandler(
		flags[messengerTopic],
		handlers.NewTopicMessageHandler(cfg.messenger, cfg.mediator),
	)

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
				api.RegisterHandlers(ctx, serviceName, handler, cfg.mediator, cfg.storage, policies)
				return nil
			}),
		),
		onstarting(func(ctx context.Context, svcCfg *appConfig) (err error) {

			svcCfg.mediator.Start(ctx)
			svcCfg.ce.Start(ctx)
			svcCfg.mc.Start(ctx)
			svcCfg.messenger.Start()

			return nil
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			// TODO: Proper cleanup
			return nil
		}),
	)

	return runner, nil
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {

	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])
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
	flag.Parse()

	return ctx, flags
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		os.Exit(1)
	}
}
