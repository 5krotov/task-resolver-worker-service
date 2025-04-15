package kafka

import (
	"context"
	"fmt"

	"worker-service/internal/domain"
)

type KafkaTaskRepository struct {
	brokers  []string
	groupID  string
	inTopic  string
	outTopic string

	producer *Producer
}

func NewKafkaTaskRepository(brokers []string, groupID, inTopic, outTopic string) (*KafkaTaskRepository, error) {
	producer, err := NewProducer(brokers)
	if err != nil {
		return nil, err
	}

	return &KafkaTaskRepository{
		brokers:  brokers,
		groupID:  groupID,
		inTopic:  inTopic,
		outTopic: outTopic,
		producer: producer,
	}, nil
}

func (k *KafkaTaskRepository) ConsumeTasks() (<-chan *domain.Task, <-chan error) {
	taskChan := make(chan *domain.Task)
	errorChan := make(chan error)

	go func() {
		StartConsumerGroup(
			context.Background(),
			k.brokers,
			k.groupID,
			k.inTopic,
			taskChan,
			errorChan,
		)
	}()

	return taskChan, errorChan
}

func (k *KafkaTaskRepository) ProduceResult(task *domain.Task) error {
	key := task.ID
	value := fmt.Sprintf("Result: %s", task.Result)
	return k.producer.SendMessage(k.outTopic, key, value)
}
