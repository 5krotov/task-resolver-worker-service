package worker

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"worker-service/internal/config"
	"worker-service/internal/infrastructure/kafka"
)

// WorkerService orchestrates the partitioned consumer and the worker pool
type WorkerService struct {
	cfg    *config.Config
	repo   *kafka.PartitionedTaskRepository
	pool   *WorkerPool
	ctx    context.Context
	cancel context.CancelFunc
}

func NewWorkerService(cfg *config.Config, repo *kafka.PartitionedTaskRepository) *WorkerService {
	ctx, cancel := context.WithCancel(context.Background())
	wp := NewWorkerPool(cfg.Worker.QueueSize, cfg.Worker.Threads, repo, cfg.Kafka.StatusTopic)
	return &WorkerService{
		cfg:    cfg,
		repo:   repo,
		pool:   wp,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ws *WorkerService) Start() error {
	// Start partitioned consumer
	if err := ws.repo.Start(); err != nil {
		return err
	}
	// Start worker pool
	ws.pool.Start()

	// Listen for tasks
	go ws.run()
	log.Println("[WorkerService] started.")
	return nil
}

func (ws *WorkerService) run() {
	taskChan := ws.repo.TaskMessages()
	errChan := ws.repo.Errors()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case msg := <-taskChan:
			// push to the worker pool
			ws.pool.Enqueue(msg)
		case err := <-errChan:
			log.Printf("[WorkerService] consumer error: %v", err)
		}
	}
}

// WaitForShutdown blocks until an OS signal is received
func (ws *WorkerService) WaitForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Println("[WorkerService] shutdown signal received.")
	ws.Stop()
}

// Stop gracefully stops the pool and the consumer
func (ws *WorkerService) Stop() {
	ws.cancel()
	ws.pool.Stop()
	ws.repo.Close()
	log.Println("[WorkerService] stopped.")
}
