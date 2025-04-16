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
	repo, err := kafka.NewPartitionedTaskRepository(cfg.Kafka.Addr, cfg.Kafka.TaskTopic, cfg)
	require.NoError(t, err, "failed to create partitioned repo")

	workerSrv := worker.NewWorkerService(cfg, repo)
	err = workerSrv.Start()
	require.NoError(t, err, "failed to start worker service")
	defer workerSrv.Stop()

	// 3) Produce tasks to "tasks" topic
	producerCfg := sarama.NewConfig()
	producerCfg.Producer.Return.Successes = true

	syncProducer, err := sarama.NewSyncProducer([]string{brokerAddr}, producerCfg)
	if err != nil {
		t.Fatalf("failed to create SyncProducer: %v", err)
	}
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
		Image: "bitnami/kafka:3.4.0",
		ExposedPorts: []string{
			"9092:9092", // Map container 9092 to host 9092
			"9093:9093", // Map container 9093 to host 9093, if needed for the controller
		},
		Env: map[string]string{
			// 1) Enable debug logging for the Bitnami scripts to see more detailed output.
			"BITNAMI_DEBUG": "true",

			// 2) Allows Kafka to use plaintext listeners for dev/testing.
			//    *NOT* recommended for production.
			"ALLOW_PLAINTEXT_LISTENER": "yes",

			// 3) Tells the Bitnami container to run Kafka in KRaft mode, no Zookeeper.
			"KAFKA_ENABLE_KRAFT": "yes",

			// 4) Node ID for this single Kafka instance (KRaft requires a node ID).
			"KAFKA_CFG_NODE_ID": "1",

			// 5) This single node acts as *both* broker and controller in KRaft.
			"KAFKA_CFG_PROCESS_ROLES": "broker,controller",

			// 6) Quorum voters: "1@(host:port)" means node ID=1 at 0.0.0.0:9093 is the sole voter.
			//    KRaft uses this for controller communication.
			"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS": "1@0.0.0.0:9093",

			// 7) Tells Kafka that the "CONTROLLER" listener name is reserved for the controller role.
			"KAFKA_CFG_CONTROLLER_LISTENER_NAMES": "CONTROLLER",

			// 8) We define two listeners inside the container:
			//    - PLAINTEXT://0.0.0.0:9092 for broker traffic
			//    - CONTROLLER://0.0.0.0:9093 for controller traffic
			"KAFKA_CFG_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",

			// 9) **Important**:
			//    Must NOT advertise the controller listener to clients.
			//    Only advertise the broker listener so clients connect to "PLAINTEXT://localhost:9092".
			"KAFKA_CFG_ADVERTISED_LISTENERS": "PLAINTEXT://localhost:9092",

			// 10) Map each listener name to a security protocol, both are PLAINTEXT in this dev scenario.
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",

			// 11) Tells Kafka that the "PLAINTEXT" listener is used for inter-broker communication.
			"KAFKA_CFG_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",

			// 12) Single-broker replication settings:
			//    1 replica for offsets, transactions, etc., because we only have one node.
			"KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",

			// 13) Let Kafka auto-create topics if they don't exist (helpful for testing).
			"KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE": "true",
		},

		WaitingFor: wait.ForListeningPort("9092/tcp").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// If container fails to start, capture logs
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
