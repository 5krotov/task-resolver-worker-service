package waiter_worker

import (
	"context"
	"fmt"
	api "github.com/5krotov/task-resolver-pkg/api/v1"
	entity "github.com/5krotov/task-resolver-pkg/entity/v1"
	"log"
	"time"
	"worker-service/internal/config"
	"worker-service/internal/future"
)

type Worker struct {
	config    config.WorkerConfig
	tasks     chan *future.TaskFuture
	timeScale time.Duration
}

func NewWorker(cfg config.WorkerConfig, tasks chan *future.TaskFuture) (*Worker, error) {
	timeScale, err := time.ParseDuration(cfg.TimeScale)
	if err != nil {
		return nil, fmt.Errorf("parse time scale failed: %v", err)
	}
	return &Worker{config: cfg, tasks: tasks, timeScale: timeScale}, nil
}

func (w *Worker) Work(ctx context.Context) {
workingLoop:
	for {
		select {
		case task := <-w.tasks:
			task.Result <- api.UpdateStatusRequest{Id: task.Task.Id, Status: entity.Status{Status: entity.STATUS_INPROGRESS, Timestamp: time.Now()}}
			time.Sleep(time.Duration(task.Task.Difficulty+1) * w.timeScale)
			task.Result <- api.UpdateStatusRequest{Id: task.Task.Id, Status: entity.Status{Status: entity.STATUS_DONE, Timestamp: time.Now()}}
			close(task.Result)
		case <-ctx.Done():
			log.Println("stopping worker ...")
			break workingLoop
		}
	}
}
