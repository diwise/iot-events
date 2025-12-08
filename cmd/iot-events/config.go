package main

import (
	"context"

	"github.com/diwise/iot-events/internal/pkg/cloudevents"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

type flagType int
type flagMap map[flagType]string

const (
	listenAddress flagType = iota
	servicePort
	controlPort
	cloudeventsFile
	policiesFile
	messengerTopic

	dbHost
	dbUser
	dbPassword
	dbPort
	dbName
	dbSSLMode

	metadataFile
)

type appConfig struct {
	storageConfig     storage.Config
	messengerConfig   messaging.Config
	cloudeventsConfig *cloudevents.Config
	cancel            context.CancelFunc
}

var oninit = servicerunner.OnInit[appConfig]
var onstarting = servicerunner.OnStarting[appConfig]
var onshutdown = servicerunner.OnShutdown[appConfig]
var webserver = servicerunner.WithHTTPServeMux[appConfig]
var muxinit = servicerunner.OnMuxInit[appConfig]
var listen = servicerunner.WithListenAddr[appConfig]
var port = servicerunner.WithPort[appConfig]
var pprof = servicerunner.WithPPROF[appConfig]
var liveness = servicerunner.WithK8SLivenessProbe[appConfig]
var readiness = servicerunner.WithK8SReadinessProbes[appConfig]
