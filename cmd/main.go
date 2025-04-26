package main

import (
	"flag"
	"log"
	"worker-service/internal/app"
	"worker-service/internal/config"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "/etc/worker-service/config.yaml", "path to config file")
	flag.Parse()

	cfg := config.NewConfig()
	err := cfg.Load(cfgPath)
	if err != nil {
		log.Fatalf("Error load config: %v\n", err)
		return
	}

	a, err := app.NewApp(*cfg)
	if err != nil {
		log.Fatalf("Error create app: %v\n", err)
		return
	}

	a.Run()
}
