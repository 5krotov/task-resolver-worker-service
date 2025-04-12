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
		log.Fatalf("Error load config: %v", err)
		return
	}

	a := app.NewApp()
	a.Run(*cfg)
}
