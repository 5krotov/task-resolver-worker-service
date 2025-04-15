package app

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"worker-service/internal/config"
	"worker-service/internal/infrastructure/kafka"
	worker "worker-service/internal/service/worker_service"
)

type App struct {
}

func NewApp() *App {
	return &App{}
}

func (*App) Run(cfg *config.Config) {
	// 2) Create PartitionedTaskRepository
	repo, err := kafka.NewPartitionedTaskRepository(cfg.Kafka.Addr, cfg.Kafka.TaskTopic)
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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
}
