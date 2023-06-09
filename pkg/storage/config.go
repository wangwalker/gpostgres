package storage

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the configuration for the storage.
type Config struct {
	// database is the name of the database.
	Database string `yaml:"database"`
	// scheme_dir is the directory where the scheme files are stored.
	SchemeDir string `yaml:"scheme_dir"`
	// data_dir is the directory where the database binary files are stored.
	DataDir string `yaml:"data_dir"`
	// index_dir is the directory where the index files are stored.
	IndexDir string `yaml:"index_dir"`
	// mode is the mode of the database. It can be "memory" or "disk".
	Mode string `yaml:"mode"`
}

func readConfig() (*Config, error) {
	var config Config
	file, err := os.Open("config.yaml")
	if err != nil {
		// possibly run in test mode
		return nil, nil
	}
	defer file.Close()
	if file != nil {
		decoder := yaml.NewDecoder(file)
		if err := decoder.Decode(&config); err != nil {
			log.Fatal(err.Error())
		}
	}
	return &config, nil
}
