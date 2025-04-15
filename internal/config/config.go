package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

// Config is the root-level config structure holding both HTTP and Kafka configuration.
type Config struct {
	HTTPConfig  `yaml:"http" validate:"required"`
	KafkaConfig `yaml:"kafka" validate:"required"`
}

// HTTPConfig holds HTTP server configuration.
type HTTPConfig struct {
	Addr string `yaml:"addr" validate:"required"`
}

// KafkaConfig holds Kafka-related configuration.
type KafkaConfig struct {
	Brokers  []string `yaml:"brokers" validate:"required"`
	GroupID  string   `yaml:"group_id" validate:"required"`
	InTopic  string   `yaml:"in_topic" validate:"required"`
	OutTopic string   `yaml:"out_topic" validate:"required"`
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) Load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}
