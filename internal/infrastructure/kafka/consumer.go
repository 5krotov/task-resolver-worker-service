package kafka

import (
	"context"
	"log"

	"github.com/IBM/sarama"
	"worker-service/internal/domain"
)

type Consumer struct {
	ready     chan bool
	taskChan  chan<- *domain.Task
	errorChan chan<- error
}

func NewConsumer(taskChan chan<- *domain.Task, errorChan chan<- error) *Consumer {
	return &Consumer{
		ready:     make(chan bool),
		taskChan:  taskChan,
		errorChan: errorChan,
	}
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		task := &domain.Task{
			ID:      string(message.Key),
			Payload: string(message.Value),
		}
		c.taskChan <- task // pass the task to the channel for processing
		session.MarkMessage(message, "")
	}
	return nil
}

// StartConsumerGroup starts a consumer group to listen for tasks
func StartConsumerGroup(
	ctx context.Context,
	brokers []string,
	groupID string,
	topic string,
	taskChan chan<- *domain.Task,
	errorChan chan<- error,
) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0

	consumer := NewConsumer(taskChan, errorChan)
	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	go func() {
		defer func() {
			if err := client.Close(); err != nil {
				log.Fatalf("Error closing client: %v", err)
			}
		}()
		for {
			if err := client.Consume(ctx, []string{topic}, consumer); err != nil {
				errorChan <- err
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-consumer.ready
	log.Println("Consumer group up and running.")
}
