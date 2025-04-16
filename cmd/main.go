package main

import (
	"log"
	"worker-service/internal/app"
	"worker-service/internal/config"
)

func main() {
	// 1) Load config
	cfg := config.NewConfig()
	err := cfg.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	a := app.NewApp()
	a.Run(cfg)
}
