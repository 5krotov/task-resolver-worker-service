package main

import (
	"log"

	"worker-service/internal/config"
	"worker-service/internal/infrastructure/kafka"
	"worker-service/internal/service/worker_service"
)

func main() {
	// 1) Load config
	cfg := config.NewConfig()
	if err := cfg.Load("config.yaml"); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2) Create PartitionedTaskRepository
	repo, err := kafka.NewPartitionedTaskRepository(
		cfg.Kafka.Addr,
		cfg.Kafka.TaskTopic,
		cfg.Kafka.StatusTopic,
	)
	if err != nil {
		log.Fatalf("Failed to create PartitionedTaskRepository: %v", err)
	}

	// 3) Create WorkerService
	workerSrv := worker.NewWorkerService(cfg, repo)

	// 4) Start WorkerService
	if err := workerSrv.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// 5) Block until shutdown signal
	workerSrv.WaitForShutdown()
	log.Println("Worker stopped gracefully.")
}
