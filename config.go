package main

import "github.com/ilyakaznacheev/cleanenv"

type Config struct {
	Port       int64  `yaml:"port"`
	Postgres   string `yaml:"postgres"`
	KafkaAddr  string `yaml:"kafkaAddr"`
	KafkaTopic string `yaml:"kafkaTopic"`
}

func GetConfig() (*Config, error) {
	var cfg Config

	err := cleanenv.ReadConfig("config.yaml", &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
