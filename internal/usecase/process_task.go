package usecase

import (
	"context"
	"log"
	"time"

	"worker-service/internal/domain"
)

// TaskProcessor simulates business logic of processing tasks. Like holding for 5s and returning back
type TaskProcessor interface {
	Process(ctx context.Context, task *domain.Task) error
}

type taskProcessor struct{}

func NewTaskProcessor() TaskProcessor {
	return &taskProcessor{}
}

func (t *taskProcessor) Process(ctx context.Context, task *domain.Task) error {
	// Simulate some work
	log.Printf("Processing task: %s with payload: %s\n", task.ID, task.Payload)
	time.Sleep(3 * time.Second) // simulating work

	// The "result" can be something derived from the payload; for now, just append to it
	task.Result = "Processed: " + task.Payload
	log.Printf("Done processing task: %s\n", task.ID)
	return nil
}
