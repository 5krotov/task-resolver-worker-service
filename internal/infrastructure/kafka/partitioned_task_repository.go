package kafka

import (
	"fmt"
	"log"
	"worker-service/internal/config"

	entity "github.com/5krotov/task-resolver-pkg/entity/v1"
)

type PartitionedTaskRepository struct {
	pConsumer *PartitionedConsumer
	producer  *Producer
	taskChan  <-chan *TaskMessage
	errChan   <-chan error
}

func NewPartitionedTaskRepository(brokerAddr, taskTopic string, cfg *config.Config) (*PartitionedTaskRepository, error) {
	taskChan := make(chan *TaskMessage, cfg.Worker.QueueSize) // arbitrary buffer
	errChan := make(chan error, 10)

	pConsumer, err := NewPartitionedConsumer(brokerAddr, taskTopic, taskChan, errChan)
	if err != nil {
		return nil, err
	}
	producer, err := NewProducer(brokerAddr)
	if err != nil {
		return nil, err
	}
	return &PartitionedTaskRepository{
		pConsumer: pConsumer,
		producer:  producer,
		taskChan:  taskChan,
		errChan:   errChan,
	}, nil
}

// Start the partitioned consumer
func (r *PartitionedTaskRepository) Start() error {
	return r.pConsumer.Start()
}

func (r *PartitionedTaskRepository) TaskMessages() <-chan *TaskMessage {
	return r.taskChan
}

func (r *PartitionedTaskRepository) Errors() <-chan error {
	return r.errChan
}

// PublishStatus writes a status update to the "statusTopic"
func (r *PartitionedTaskRepository) PublishStatus(statusTopic string, id int64, status entity.Status) error {
	key := fmt.Sprintf("%d", id)
	payload := map[string]interface{}{
		"id":     id,
		"status": status,
		"times":  status.Timestamp,
	}
	return r.producer.SendMessage(statusTopic, key, payload)
}

func (r *PartitionedTaskRepository) Close() {
	r.pConsumer.Stop()
	if err := r.producer.Close(); err != nil {
		log.Printf("[PartitionedTaskRepository] error closing producer: %v", err)
	}
}
