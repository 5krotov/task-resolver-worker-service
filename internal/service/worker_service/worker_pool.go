package worker

import (
	"log"
	"sync"
	"time"

	entity "github.com/5krotov/task-resolver-pkg/entity/v1"

	"worker-service/internal/domain"
	"worker-service/internal/infrastructure/kafka"
)

type WorkerPool struct {
	queueSize   int
	threads     int
	queue       chan *kafka.TaskMessage
	wg          sync.WaitGroup
	repo        *kafka.PartitionedTaskRepository
	statusTopic string
}

func NewWorkerPool(queueSize, threads int, repo *kafka.PartitionedTaskRepository, statusTopic string) *WorkerPool {
	return &WorkerPool{
		queueSize:   queueSize,
		threads:     threads,
		queue:       make(chan *kafka.TaskMessage, queueSize),
		repo:        repo,
		statusTopic: statusTopic,
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.threads; i++ {
		wp.wg.Add(1)
		go wp.workerLoop(i)
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.queue)
	wp.wg.Wait()
}

func (wp *WorkerPool) Enqueue(msg *kafka.TaskMessage) {
	wp.queue <- msg
}

func (wp *WorkerPool) workerLoop(workerID int) {
	defer wp.wg.Done()

	for msg := range wp.queue {
		wp.processTask(workerID, msg)
	}
}

func (wp *WorkerPool) processTask(workerID int, msg *kafka.TaskMessage) {
	task := msg.TaskData
	log.Printf("[Worker %d] partition=%d offset=%d => ID=%d, difficulty=%d",
		workerID, msg.Partition, msg.Offset, task.Id, task.Difficulty)

	// 1) Publish status = PENDING
	now := time.Now()
	err := wp.repo.PublishStatus(wp.statusTopic, task.Id, entity.Status{
		Status:    domain.StatusPending,
		Timestamp: now,
	})
	if err != nil {
		log.Printf("[Worker %d] error publishing PENDING: %v", workerID, err)
		return
	}

	// 2) IN_PROGRESS
	err = wp.repo.PublishStatus(wp.statusTopic, task.Id, entity.Status{
		Status:    domain.StatusInProgress,
		Timestamp: time.Now(),
	})
	if err != nil {
		log.Printf("[Worker %d] error publishing IN_PROGRESS: %v", workerID, err)
		return
	}

	// 3) Do the "work" (sleep by difficulty seconds)
	log.Printf("[Worker %d] processing task ID=%d for %d seconds...", workerID, task.Id, task.Difficulty)
	time.Sleep(time.Duration(task.Difficulty) * time.Second)

	// 4) DONE
	err = wp.repo.PublishStatus(wp.statusTopic, task.Id, entity.Status{
		Status:    domain.StatusDone,
		Timestamp: time.Now(),
	})
	if err != nil {
		log.Printf("[Worker %d] error publishing DONE: %v", workerID, err)
		return
	}

	log.Printf("[Worker %d] partition=%d offset=%d => Task ID=%d done.",
		workerID, msg.Partition, msg.Offset, task.Id)
}
