package tsb

import (
	"encoding/json"
	"io"
	"os"
)

type DBConfig struct {
	Hostname string `json:"hostname"`
	DBName   string `json:"dbname"`
	User     string `json:"user"`
	Port     int    `json:"port"`
}

func loadConfig(filename string) (DBConfig, error) {
	var config DBConfig
	configFile, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	defer configFile.Close()
	bytes, err := io.ReadAll(configFile)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(bytes, &config)
	return config, err
}