package kafka

import (
	"encoding/json"
	"fmt"
	entity "github.com/5krotov/task-resolver-pkg/entity/v1"
	"github.com/IBM/sarama"
	"log"
	"sync"
)

// TaskMessage holds data from a partition plus the raw msg
type TaskMessage struct {
	Partition int32
	Offset    int64
	TaskData  entity.Task
}

// PartitionedConsumer spawns one goroutine per partition
type PartitionedConsumer struct {
	consumer  sarama.Consumer
	taskTopic string
	taskChan  chan<- *TaskMessage
	errorChan chan<- error
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// NewPartitionedConsumer sets up a non-group consumer for a single topic
func NewPartitionedConsumer(
	brokerAddr, taskTopic string,
	taskChan chan<- *TaskMessage,
	errorChan chan<- error,
) (*PartitionedConsumer, error) {

	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	// No offset commits for non-group consumer
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama consumer: %w", err)
	}

	pc := &PartitionedConsumer{
		consumer:  consumer,
		taskTopic: taskTopic,
		taskChan:  taskChan,
		errorChan: errorChan,
		stopChan:  make(chan struct{}),
	}
	return pc, nil
}

// Start enumerates partitions and spawns a goroutine per partition
func (pc *PartitionedConsumer) Start() error {
	partitions, err := pc.consumer.Partitions(pc.taskTopic)
	if err != nil {
		return fmt.Errorf("failed to get partitions for topic %s: %w", pc.taskTopic, err)
	}
	log.Printf("[PartitionedConsumer] Found %d partitions for topic=%s", len(partitions), pc.taskTopic)

	for _, partition := range partitions {
		pc.wg.Add(1)
		go pc.consumePartition(partition)
	}
	return nil
}

// consumePartition consumes messages from a single partition in a loop
func (pc *PartitionedConsumer) consumePartition(part int32) {
	defer pc.wg.Done()

	// Start from the oldest offset (or you could use sarama.OffsetNewest)
	pConsumer, err := pc.consumer.ConsumePartition(pc.taskTopic, part, sarama.OffsetOldest)
	if err != nil {
		pc.errorChan <- fmt.Errorf("failed to consume partition %d: %w", part, err)
		return
	}
	defer pConsumer.Close()

	log.Printf("[PartitionedConsumer] Start reading partition=%d ...", part)

	for {
		select {
		case msg, ok := <-pConsumer.Messages():
			if !ok {
				log.Printf("[PartitionedConsumer] partition=%d channel closed", part)
				return
			}
			var task entity.Task
			if err := json.Unmarshal(msg.Value, &task); err != nil {
				pc.errorChan <- fmt.Errorf("partition=%d offset=%d invalid JSON: %w", part, msg.Offset, err)
				continue
			}
			pc.taskChan <- &TaskMessage{
				Partition: part,
				Offset:    msg.Offset,
				TaskData:  task,
			}

		case <-pc.stopChan:
			log.Printf("[PartitionedConsumer] partition=%d stop signal", part)
			return
		}
	}
}

// Stop signals all partition goroutines to exit and waits for them
func (pc *PartitionedConsumer) Stop() {
	close(pc.stopChan)
	pc.wg.Wait()
	if err := pc.consumer.Close(); err != nil {
		log.Printf("[PartitionedConsumer] error closing consumer: %v", err)
	}
	log.Printf("[PartitionedConsumer] stopped.")
}
