package mqtt

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/diwise/iot-events/internal/pkg/devicemanagement"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type Config struct {
	Enabled    bool
	BrokerUrl  string
	Username   string
	Password   string
	Topics     []string
	ClientId   string
	Insecure   bool
	Prefix     string
	Identifier string
}

func NewConfig(enabled bool, brokerUrl string, user string, password string, topics []string, clientId string, insecure bool, prefix, identifier string) Config {
	if clientId == "" {
		clientId = uuid.NewString()
	}

	return Config{
		Enabled:    enabled,
		Username:   user,
		Password:   password,
		Topics:     topics,
		ClientId:   clientId,
		BrokerUrl:  brokerUrl,
		Insecure:   insecure,
		Prefix:     prefix,
		Identifier: identifier,
	}
}

type mqttClient struct {
	pub     chan TopicMessage
	errmsg  chan TopicMessage
	close   chan struct{}
	pc      paho.Client
	started atomic.Bool
	enabled atomic.Bool
}

//go:generate moq -rm -out mqtt_mock.go . Client
type Client interface {
	Start(ctx context.Context)
	Publish(ctx context.Context, msg TopicMessage) error
	Enabled() bool
}

func NewClient(ctx context.Context, cfg Config) (Client, error) {
	log := logging.GetFromContext(ctx)

	if !cfg.Enabled {
		log.Info("MQTT client is disabled")
		return &ClientMock{
			StartFunc: func(ctx context.Context) {},
			PublishFunc: func(ctx context.Context, msg TopicMessage) error {
				return nil
			},
			EnabledFunc: func() bool { return false },
		}, nil
	}

	options := paho.NewClientOptions()

	options.AddBroker(cfg.BrokerUrl)
	options.SetUsername(cfg.Username)
	options.SetPassword(cfg.Password)
	options.SetClientID(cfg.ClientId)
	options.SetDefaultPublishHandler(func(client paho.Client, msg paho.Message) {})
	options.SetKeepAlive(time.Duration(10) * time.Second)
	options.AutoReconnect = true
	options.SetConnectRetry(true)
	options.SetConnectRetryInterval(5 * time.Second)

	options.OnConnect = func(mc paho.Client) {
		log.Info("connected to mqtt broker", "broker", cfg.BrokerUrl)
		for _, topic := range cfg.Topics {
			log.Info("subscribing to topic", "topic", topic)
			token := mc.Subscribe(topic, 0, nil)
			token.Wait()
		}
	}

	options.OnReconnecting = func(mc paho.Client, co *paho.ClientOptions) {
		log.Info("reconnecting to mqtt broker", "broker", cfg.BrokerUrl)
	}

	options.OnConnectionLost = func(mc paho.Client, err error) {
		log.Warn("connection lost", "err", err.Error())
	}

	if cfg.Insecure {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	pc := paho.NewClient(options)

	c := &mqttClient{
		pub:     make(chan TopicMessage),
		errmsg:  make(chan TopicMessage),
		close:   make(chan struct{}),
		pc:      pc,
		started: atomic.Bool{},
		enabled: atomic.Bool{},
	}

	c.enabled.Store(true)

	return c, nil
}

func (c *mqttClient) Enabled() bool {
	return c.enabled.Load()
}

func (c *mqttClient) Start(ctx context.Context) {
	log := logging.GetFromContext(ctx)

	if !c.enabled.Load() {
		log.Info("mqtt has been explicitly disabled with MQTT_ENABLED=false and will therefore not start")
		return
	}

	log.Info("starting mqtt client...")

	go c.retryPublish(ctx)
	go c.run(ctx)
}

func (c *mqttClient) retryPublish(ctx context.Context) {
	log := logging.GetFromContext(ctx)

	log.Debug("starting MQTT retry channel...")

	for {
		select {
		case <-c.close:
			for {
				select {
				case m := <-c.errmsg:
					log.Warn("dropping failed message because client is shutting down...", "topic", m.TopicName())
				default:
					close(c.errmsg)
					close(c.close)
					return
				}
			}
		case m := <-c.errmsg:
			if m.Retry() >= 3 {
				log.Error("dropping mqtt message after 3 failed attempts", "topic", m.TopicName())
				continue
			}

			go func() {
				select {
				case <-c.close:
					log.Warn("dropping failed message because client is shutting down...", "topic", m.TopicName())
					return
				default:
					select {
					case <-c.close:
						return
					default:
						time.AfterFunc(time.Duration(m.Retry())*time.Second, func() {
							c.pub <- m
						})
					}
				}
			}()
		}
	}
}

func (c *mqttClient) run(ctx context.Context) error {
	log := logging.GetFromContext(ctx)
	c.started.Store(true)

	connect := func() error {
		if c.pc.IsConnectionOpen() {
			return nil
		}

		if token := c.pc.Connect(); token.Wait() && token.Error() != nil {
			log.Error("connection error", "err", token.Error())
			return token.Error()
		}

		return nil
	}

	log.Debug("MQTT client is running...")

	for {
		select {
		case <-ctx.Done():
			c.started.Store(false)

			for {
				select {
				case m := <-c.pub:
					log.Warn("dropping message because client is shutting down...", "topic", m.TopicName())
				default:
					close(c.pub)
					c.pc.Disconnect(100)
					c.close <- struct{}{}
					return nil
				}
			}

		case m := <-c.pub:
			if !c.started.Load() {
				log.Warn("mqtt client is not started, cannot publish message", "topic", m.TopicName())
				continue
			}

			if err := connect(); err != nil {
				log.Error("client is not connected to broker", "err", err.Error())
				continue
			}

			token := c.pc.Publish(m.TopicName(), 0, false, m.Body())
			token.Wait()

			if err := token.Error(); err != nil {
				log.Error("failed to publish mqtt message", "topic", m.TopicName(), "err", err.Error())
				m.Err(err)
				c.errmsg <- m
			} else {
				log.Debug("mqtt message published", "topic", m.TopicName())
			}
		}
	}
}

func (c *mqttClient) Publish(ctx context.Context, msg TopicMessage) error {
	log := logging.GetFromContext(ctx)

	if !c.started.Load() {
		log.Warn("mqtt client not started, cannot publish message", "topic", msg.TopicName())
		return fmt.Errorf("mqtt client not started")
	}

	c.pub <- msg

	return nil
}

type TopicMessage interface {
	Body() []byte
	ContentType() string
	TopicName() string
	Retry() int
	Err(err error)
}

func newTopicMessage(topic string, payload any) TopicMessage {
	return &topicMessage{
		topic:   topic,
		payload: payload,
	}
}

type topicMessage struct {
	topic   string
	payload any
	retry   int
}

func (t *topicMessage) Err(err error) {
	t.retry++
}

func (t *topicMessage) Retry() int {
	return t.retry
}

func (t *topicMessage) Body() []byte {
	b, _ := json.Marshal(t.payload)
	return b
}
func (t *topicMessage) TopicName() string {
	return t.topic
}
func (t *topicMessage) ContentType() string {
	return "application/json"
}

type mqttPublisher struct {
	m          mediator.Mediator
	mqttClient Client
	dmc        devicemanagement.Client
	prefix     string
	identifier string
}

func Start(ctx context.Context, m mediator.Mediator, mqttClient Client, prefix, identifier string, dmc devicemanagement.Client) error {
	log := logging.GetFromContext(ctx)

	if !mqttClient.Enabled() {
		log.Info("mqtt client is disabled, skipping mqtt subscriber registration")
		return nil
	}

	prefix = strings.TrimSuffix(prefix, "/")

	p := &mqttPublisher{
		m:          m,
		mqttClient: mqttClient,
		dmc:        dmc,
		prefix:     prefix,
		identifier: identifier,
	}

	mqttClient.Start(ctx)
	messageAccepted := newSubscriber("message.accepted", p.newMessageAcceptedHandler)
	statusMessage := newSubscriber("device-status", p.newStatusMessageHandler)

	m.Register(messageAccepted)
	m.Register(statusMessage)

	go messageAccepted.run(ctx)
	go statusMessage.run(ctx)

	return nil
}

var mapper = map[string]string{
	"/3/9":       "soc",          // device (SOC = State of Charge = battery level %)
	"/3/7":       "vbat",         //
	"/3301/5700": "illuminance",  // illuminance
	"/3303/5700": "temperature",  // temperature
	"/3304/5700": "humidity",     // humidity
	"/3435/3":    "fillinglevel", // filling level
	"/3428/1":    "pm10",         // air quality
	"/3428/3":    "pm25",         //
	"/3428/15":   "no2",          //
	"/3428/17":   "co2",          //
	"/3424/1":    "volume",       // water meter
	"/3411/1":    "level",        // battery
	"/3411/2":    "capacity",     //
	"/3411/3":    "voltage",      //
	"/3200/5500": "state",        // digital input
	"/3200/5501": "count",        //
	"/3434/1":    "count",        // people counter
	"/3302/5500": "presence",     // presence
	"/3302/5501": "count",        //
	"/3330/5700": "distance",     // distance
	"/3323/5700": "pressure",     // pressure
	"/3327/5700": "conductivity", // conductivity
	"/3328/5700": "power",        // power
	"/3331/5700": "energy",       // energy
}

func (p *mqttPublisher) publish(ctx context.Context, topic string, v any) error {
	return p.mqttClient.Publish(ctx, newTopicMessage(topic, v))
}

type valueMessage struct {
	Device    device    `json:"device"`
	Type      string    `json:"type"`
	Unit      string    `json:"unit,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Value     any       `json:"value"`
}

type device struct {
	DeviceID string `json:"deviceID,omitempty"`
	SensorID string `json:"sensorID,omitempty"`
	Name     string `json:"name,omitempty"`
}

func (d *device) Property(prop string) string {
	switch strings.ToLower(prop) {
	case "deviceid":
		return d.DeviceID
	case "sensorid":
		return d.SensorID
	case "name":
		if d.Name != "" {
			return d.Name
		}
		return d.DeviceID
	default:
		return d.DeviceID
	}
}

var re = regexp.MustCompile(`[^A-Za-z0-9/_-]+`)

func (p *mqttPublisher) newMessageAcceptedHandler(m mediator.Message) {
	ctx := m.Context()
	log := logging.GetFromContext(ctx).With(slog.String("message_type", m.Type()))

	tenant := m.Tenant()

	if tenant == "" {
		log.Error("message contains no tenant")
		return
	}

	topicMessage := struct {
		Pack *senml.Pack `json:"pack,omitempty"`
	}{}

	err := json.Unmarshal(m.Data(), &topicMessage)
	if err != nil {
		log.Error("failed to unmarshal message", "err", err.Error())
		return
	}

	if topicMessage.Pack == nil {
		log.Error("message contains no senml pack")
		return
	}

	if err := topicMessage.Pack.Validate(); err != nil {
		log.Error("invalid senml pack", "err", err.Error())
		return
	}

	pack := topicMessage.Pack.Clone()
	pack.Normalize()

	getDeviceID := func(p senml.Pack) string {
		if len(p) == 0 {
			return ""
		}
		parts := strings.Split(p[0].Name, "/")
		return parts[0]
	}

	getPort := func(p senml.Pack) string {
		if len(p) == 0 {
			return ""
		}

		parts := strings.Split(p[0].Name, "/")
		if len(parts) != 4 {
			return ""
		}
		return parts[1]
	}

	getObjectID := func(r senml.Record) string {
		parts := strings.Split(r.Name, "/")

		if len(parts) < 2 {
			return ""
		}

		return fmt.Sprintf("/%s/%s", parts[len(parts)-2], parts[len(parts)-1])
	}

	deviceID := getDeviceID(pack)

	if deviceID == "" {
		log.Error("senml pack contains no device id")
		return
	}

	d, err := p.dmc.GetDevice(ctx, deviceID)
	if err != nil {
		log.Error("failed to get device", "err", err.Error())
		return
	}

	dev := device{
		DeviceID: d.DeviceID,
		SensorID: d.SensorID,
		Name:     d.Name,
	}

	topicIdentifier := re.ReplaceAllString(dev.Property(p.identifier), "-")

	if port := getPort(pack); port != "" {
		topicIdentifier = topicIdentifier + "/" + port
	}

	for _, rec := range pack {
		objectID := getObjectID(rec)

		if name, ok := mapper[objectID]; ok {
			var topic = fmt.Sprintf("%s/%s/%s/%s", p.prefix, tenant, topicIdentifier, name)

			v := valueMessage{
				Device:    dev,
				Type:      name,
				Unit:      rec.Unit,
				Timestamp: time.Unix(int64(rec.Time), 0),
			}

			if rec.Value != nil {
				v.Value = *rec.Value
			} else if rec.BoolValue != nil {
				v.Value = *rec.BoolValue
			} else if rec.StringValue != "" {
				v.Value = rec.StringValue
			}

			err = p.publish(ctx, topic, v)
			if err != nil {
				log.Error("failed to publish mqtt message", "topic", topic, "err", err.Error())
				return
			}

			log.Debug("mqtt message published", "topic", topic)
		} else {
			log.Debug("no mapping found for object id, skipping...", "object_id", objectID)
		}
	}
}

type statusMessage struct {
	Device          device    `json:"device"`
	RSSI            *float64  `json:"rssi,omitempty"`
	LoRaSNR         *float64  `json:"loRaSNR,omitempty"`
	Frequency       *int64    `json:"frequency,omitempty"`
	SpreadingFactor *float64  `json:"spreadingFactor,omitempty"`
	DR              *int      `json:"dr,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
}

func (p *mqttPublisher) newStatusMessageHandler(m mediator.Message) {
	ctx := m.Context()
	log := logging.GetFromContext(ctx)

	tenant := m.Tenant()
	if tenant == "" {
		log.Error("status message contains no tenant")
		return
	}

	topicMessage := struct {
		DeviceID        string    `json:"deviceID,omitempty"`
		RSSI            *float64  `json:"rssi,omitempty"`
		LoRaSNR         *float64  `json:"loRaSNR,omitempty"`
		Frequency       *int64    `json:"frequency,omitempty"`
		SpreadingFactor *float64  `json:"spreadingFactor,omitempty"`
		DR              *int      `json:"dr,omitempty"`
		Timestamp       time.Time `json:"timestamp"`
	}{}

	err := json.Unmarshal(m.Data(), &topicMessage)
	if err != nil {
		log.Error("failed to unmarshal status message", "err", err.Error())
		return
	}

	d, err := p.dmc.GetDevice(ctx, topicMessage.DeviceID)
	if err != nil {
		log.Error("failed to get device", "err", err.Error())
		return
	}

	dev := device{
		DeviceID: d.DeviceID,
		SensorID: d.SensorID,
		Name:     d.Name,
	}

	sm := statusMessage{
		Device:          dev,
		RSSI:            topicMessage.RSSI,
		LoRaSNR:         topicMessage.LoRaSNR,
		Frequency:       topicMessage.Frequency,
		SpreadingFactor: topicMessage.SpreadingFactor,
		DR:              topicMessage.DR,
		Timestamp:       topicMessage.Timestamp,
	}

	topicIdentifier := re.ReplaceAllString(dev.Property(p.identifier), "-")

	var topic = fmt.Sprintf("%s/%s/%s/status", p.prefix, tenant, topicIdentifier)

	err = p.publish(ctx, topic, sm)
	if err != nil {
		log.Error("failed to publish mqtt status message", "err", err.Error())
		return
	}

	log.Debug("mqtt status message published", "topic", topic)
}

type mqttSubscriber struct {
	topic   string
	inbox   chan mediator.Message
	handler func(mediator.Message)
}

func newSubscriber(topic string, handlerFunc func(mediator.Message)) *mqttSubscriber {
	return &mqttSubscriber{
		inbox:   make(chan mediator.Message),
		topic:   topic,
		handler: handlerFunc,
	}
}

func (s *mqttSubscriber) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-s.inbox:
			s.handler(m)
		}
	}
}

func (s *mqttSubscriber) ID() string {
	return "mqtt-subscriber-" + s.topic
}

func (s *mqttSubscriber) Mailbox() chan mediator.Message {
	return s.inbox
}

func (s *mqttSubscriber) Handle(m mediator.Message) bool {
	if m.Type() == s.topic {
		s.inbox <- m
		return true
	}

	return false
}
