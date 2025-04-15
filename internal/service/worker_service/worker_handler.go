package worker_service

import (
	"context"
	"log"

	"worker-service/internal/domain"
	"worker-service/internal/usecase"
)

type WorkerHandler struct {
	taskRepo      domain.TaskRepository
	taskProcessor usecase.TaskProcessor
}

func NewWorkerHandler(taskRepo domain.TaskRepository, processor usecase.TaskProcessor) *WorkerHandler {
	return &WorkerHandler{
		taskRepo:      taskRepo,
		taskProcessor: processor,
	}
}

func (h *WorkerHandler) Start() {
	// Start consuming tasks
	taskChan, errorChan := h.taskRepo.ConsumeTasks()

	go func() {
		for {
			select {
			case task := <-taskChan:
				if task == nil {
					continue
				}
				err := h.taskProcessor.Process(context.Background(), task)
				if err != nil {
					log.Printf("Error processing task %s: %v\n", task.ID, err)
					continue
				}
				// Now produce the result to Kafka
				err = h.taskRepo.ProduceResult(task)
				if err != nil {
					log.Printf("Error producing result for task %s: %v\n", task.ID, err)
				}
			case err := <-errorChan:
				log.Printf("Error from consumer: %v\n", err)
			}
		}
	}()
}
