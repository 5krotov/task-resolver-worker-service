package kafka_repository

import (
	"context"
	"fmt"
	"github.com/5krotov/task-resolver-pkg/utils"
	"github.com/IBM/sarama"
	"log"
	"math"
	"time"
	"worker-service/internal/config"
	"worker-service/internal/future"
)

type KafkaRepository struct {
	config              config.KafkaConfig
	addr                string
	taskTopic           string
	statusTopic         string
	groupId             string
	timeout             time.Duration
	timeoutMilliseconds int
	consumer            *sarama.ConsumerGroup
	producer            *sarama.SyncProducer
	handler             *Handler
}

func NewKafkaRepository(cfg config.KafkaConfig, futureTasks chan *future.TaskFuture) (*KafkaRepository, error) {
	addr := utils.GetEnvVar(cfg.Addr)
	taskTopic := utils.GetEnvVar(cfg.TaskTopic)
	statusTopic := utils.GetEnvVar(cfg.StatusTopic)
	groupId := utils.GetEnvVar(cfg.Group)
	timeout, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("parse timeout failed: %v", err)
	}
	timeoutMillisecondsBig := timeout.Milliseconds()
	timeoutMilliseconds := math.MaxInt
	if timeoutMillisecondsBig < int64(math.MinInt) {
		timeoutMilliseconds = math.MinInt
	}
	if timeoutMillisecondsBig > int64(math.MaxInt) || timeoutMillisecondsBig < int64(math.MinInt) {
		timeoutMilliseconds = int(timeoutMillisecondsBig)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = cfg.Retries
	saramaConfig.Producer.Return.Successes = true

	consumer, err := sarama.NewConsumerGroup([]string{addr}, groupId, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create consumer failed: %v", err)
	}

	producer, err := sarama.NewSyncProducer([]string{addr}, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create producer failed: %v", err)
	}

	handler := &Handler{StatusTopic: statusTopic, Producer: producer, Tasks: futureTasks}

	return &KafkaRepository{
			config:              cfg,
			addr:                addr,
			taskTopic:           taskTopic,
			statusTopic:         statusTopic,
			groupId:             groupId,
			consumer:            &consumer,
			producer:            &producer,
			timeout:             timeout,
			timeoutMilliseconds: timeoutMilliseconds,
			handler:             handler,
		},
		nil
}

func (r *KafkaRepository) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("stopping repository ...")
			r.Close()
		default:
			err := (*r.consumer).Consume(
				ctx,
				[]string{r.taskTopic},
				r.handler,
			)
			if err != nil {
				log.Printf("consume message error: %v", err)
			}
		}
	}
}

func (r *KafkaRepository) Close() {
	err := (*r.producer).Close()
	if err != nil {
		log.Printf("failed close producer: %v", err)
	}
	err = (*r.consumer).Close()
	if err != nil {
		log.Printf("failed close consumer: %v", err)
	}
}
