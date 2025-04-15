package tests

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"worker-service/internal/config"
	"worker-service/internal/infrastructure/kafka"
	"worker-service/internal/service/worker_service"
)

func TestPartitionedWorkerIntegration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	kafkaC, err := startKafkaContainerWithLogs(ctx)
	require.NoError(t, err, "failed to start Kafka container")

	// Ensure the container is removed at the end
	defer func() {
		_ = kafkaC.Terminate(ctx)
	}()

	// 1) Retrieve host/port
	host, err := kafkaC.Host(ctx)
	require.NoError(t, err, "failed to get container host")

	mappedPort, err := kafkaC.MappedPort(ctx, "9092/tcp")
	require.NoError(t, err, "failed to get mapped port for 9092")

	brokerAddr := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	// Quick check that the port is open
	conn, dialErr := net.DialTimeout("tcp", brokerAddr, 10*time.Second)
	require.NoError(t, dialErr, "could not dial Kafka at %s", brokerAddr)
	_ = conn.Close()

	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Addr:        brokerAddr,
			TaskTopic:   "tasks",
			StatusTopic: "task-status",
		},
		Worker: config.WorkerConfig{
			QueueSize: 5,
			Threads:   2,
		},
	}

	// 2) Start your partitioned worker
	repo, err := kafka.NewPartitionedTaskRepository(cfg.Kafka.Addr, cfg.Kafka.TaskTopic, cfg.Kafka.StatusTopic)
	require.NoError(t, err, "failed to create partitioned repo")

	workerSrv := worker.NewWorkerService(cfg, repo)
	err = workerSrv.Start()
	require.NoError(t, err, "failed to start worker service")
	defer workerSrv.Stop()

	// 3) Produce tasks to "tasks" topic
	syncProducer, err := sarama.NewSyncProducer([]string{brokerAddr}, sarama.NewConfig())
	require.NoError(t, err, "failed to create SyncProducer")
	defer syncProducer.Close()

	require.NoError(t, produceTestTask(syncProducer, cfg.Kafka.TaskTopic, 1001, 1))
	require.NoError(t, produceTestTask(syncProducer, cfg.Kafka.TaskTopic, 1002, 2))

	// 4) Consume from "task-status"
	simpleConsumer, err := sarama.NewConsumer([]string{brokerAddr}, sarama.NewConfig())
	require.NoError(t, err, "failed to create simple consumer")
	defer simpleConsumer.Close()

	pc, err := simpleConsumer.ConsumePartition(cfg.Kafka.StatusTopic, 0, sarama.OffsetOldest)
	require.NoError(t, err, "failed to consume partition=0 of status topic")
	defer pc.Close()

	// Expect 3 statuses per task => 6 total
	expected := 6
	received := 0
	timeout := time.After(30 * time.Second)

loop:
	for {
		select {
		case msg := <-pc.Messages():
			if msg != nil {
				received++
				log.Printf("[test] status => key=%s, value=%s", string(msg.Key), string(msg.Value))
				if received >= expected {
					break loop
				}
			}
		case <-timeout:
			break loop
		}
	}

	require.GreaterOrEqual(t, received, expected, "did not receive enough status messages")
}

// startKafkaContainerWithLogs tries to start the container. If it fails,
// it reads the container logs and returns an error with that output.
func startKafkaContainerWithLogs(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "bitnami/kafka:3.4.0",
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"ALLOW_PLAINTEXT_LISTENER": "yes",
			// Example KRaft single-node config
			"KAFKA_ENABLE_KRAFT":                       "yes",
			"KAFKA_CFG_NODE_ID":                        "1",
			"KAFKA_CFG_PROCESS_ROLES":                  "broker,controller",
			"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS":       "1@127.0.0.1:9093",
			"KAFKA_CFG_LISTENERS":                      "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_CFG_ADVERTISED_LISTENERS":           "PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093",
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CFG_INTER_BROKER_LISTENER_NAME":     "PLAINTEXT",

			"KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE":                "true",
		},
		WaitingFor: wait.ForListeningPort("9092/tcp").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// If container fails to start, grab logs
		logs, logsErr := tryReadLogs(ctx, container)
		if logsErr != nil {
			return nil, fmt.Errorf("start container: %w; also could not read logs: %v", err, logsErr)
		}
		return nil, fmt.Errorf("start container: %w\n=== CONTAINER LOGS ===\n%s", err, logs)
	}
	return container, nil
}

func tryReadLogs(ctx context.Context, c testcontainers.Container) (string, error) {
	reader, err := c.Logs(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	bytes, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func produceTestTask(producer sarama.SyncProducer, topic string, id int64, difficulty int) error {
	payload := fmt.Sprintf(`{"id":%d,"difficulty":%d}`, id, difficulty)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", id)),
		Value: sarama.ByteEncoder(payload),
	}
	_, _, err := producer.SendMessage(msg)
	return err
}
