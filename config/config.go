package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Broker  string `yaml:"broker"`
	Topic   string `yaml:"topic"`
	GroupID string `yaml:"groupID"`
}

// ApplyConfig reads data from config.yaml file , parse it and return Broker, Topic and GroupID
func ApplyConfig() (string, string, string, error) {

	f, err := os.ReadFile("C:/Users/akshraj.chavda/Internship/Go/project/Go_Project/config.yaml")
	if err != nil {
		return "", "", "",
			fmt.Errorf("failed to read config file: %w", err)
	}

	var c Config

	if err := yaml.Unmarshal(f, &c); err != nil {
		return "", "", "",
			fmt.Errorf("failed to Unmarshal yaml: %w", err)
	}

	fmt.Println("YAML Configuration successful")
	return c.Broker, c.Topic, c.GroupID, nil
}
