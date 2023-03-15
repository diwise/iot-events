package cloudevents

import (
	"io"
	"os"

	yaml "gopkg.in/yaml.v2"
)

type EntityInfo struct {
	IDPattern string `yaml:"idPattern"`
}

type Subscriber struct {
	ID        string       `yaml:"id"`
	Name      string       `yaml:"name"`
	Type      string       `yaml:"type"`
	Endpoint  string       `yaml:"endpoint"`
	Tenants   []string     `yaml:"tenants"`
	Entities  []EntityInfo `yaml:"entities"`
	Source    string       `yaml:"source"`
	EventType string       `yaml:"eventType"`
}

type Config struct {
	Subscribers []Subscriber `yaml:"subscribers"`
}

func LoadConfiguration(data io.Reader) (*Config, error) {
	buf, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}

	cfg := Config{}
	if err := yaml.Unmarshal(buf, &cfg); err == nil {
		return &cfg, nil
	} else {
		return nil, err
	}
}

func LoadConfigurationFromFile(filepath string) *Config {
	configFile, err := os.Open(filepath)
	if err != nil {
		return &Config{}
	}
	defer configFile.Close()

	config, err := LoadConfiguration(configFile)
	if err != nil {
		return &Config{}
	}

	return config
}
