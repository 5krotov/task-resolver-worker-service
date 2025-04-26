package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"worker-service/internal/config"
	"worker-service/internal/future"
	kafka_repository "worker-service/internal/repository/kafka"
	"worker-service/internal/service"
	waiter_worker "worker-service/internal/worker/waiter"
)

type App struct {
	service *service.Service
}

func NewApp(cfg config.Config) (*App, error) {

	futureTasks := make(chan *future.TaskFuture)

	rep, err := kafka_repository.NewKafkaRepository(cfg.KafkaConfig, futureTasks)
	if err != nil {
		return nil, fmt.Errorf("create repository failed: %v", err)
	}

	wrk, err := waiter_worker.NewWorker(cfg.WorkerConfig, futureTasks)
	if err != nil {
		return nil, fmt.Errorf("create worker failed: %v", err)
	}

	srv := service.NewService(rep, wrk)

	log.Println("app created")

	return &App{service: srv}, nil
}

func (a *App) Run() {
	ctx, cancel := context.WithCancel(context.Background())

	log.Println("serve ...")

	a.service.Serve(ctx)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancel()
}
