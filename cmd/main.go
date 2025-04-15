package main

import (
	"log"
	"net/http"

	"worker-service/internal/config"
	"worker-service/internal/infrastructure/kafka"
	"worker-service/internal/service"
	"worker-service/internal/service/hello_world_service"
	"worker-service/internal/service/worker_service"
	"worker-service/internal/usecase"
)

func main() {
	// 1) Create a new config and load from file
	cfg := config.NewConfig()
	err := cfg.Load("config.yaml") // or the path to your actual YAML file
	if err != nil {
		log.Fatalf("Unable to load config: %v", err)
	}

	// 2) Create KafkaTaskRepository using values from cfg.KafkaConfig
	repo, err := kafka.NewKafkaTaskRepository(
		cfg.KafkaConfig.Brokers,
		cfg.KafkaConfig.GroupID,
		cfg.KafkaConfig.InTopic,
		cfg.KafkaConfig.OutTopic,
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka repository: %v", err)
	}

	// 3) Create TaskProcessor
	processor := usecase.NewTaskProcessor()

	// 4) Initialize WorkerHandler and start consuming tasks
	workerHandler := worker_service.NewWorkerHandler(repo, processor)
	workerHandler.Start()

	// 5) Create the worker service
	workerSrv := worker_service.NewWorkerService(workerHandler)

	// 6) (Optional) HelloWorldService
	helloSrv := hello_world_service.NewHelloWorldService()

	// 7) Combine services in an HTTP server
	mux := http.NewServeMux()
	registerServices(mux, workerSrv, helloSrv)

	log.Printf("Starting HTTP server on %s", cfg.HTTPConfig.Addr)
	err = http.ListenAndServe(cfg.HTTPConfig.Addr, mux)
	if err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func registerServices(mux *http.ServeMux, services ...service.Service) {
	for _, s := range services {
		s.Register(mux)
	}
}
