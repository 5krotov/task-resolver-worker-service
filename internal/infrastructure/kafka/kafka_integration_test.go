package kafka_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"worker-service/internal/domain"
	"worker-service/internal/infrastructure/kafka"
)

func TestKafkaIntegration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1) Build ContainerRequest to run a Kafka container
	containerReq := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.3.3",
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"ALLOW_PLAINTEXT_LISTENER":        "yes",
			"KAFKA_LISTENERS":                 "PLAINTEXT://0.0.0.0:9092",
			"KAFKA_ADVERTISED_LISTENERS":      "PLAINTEXT://localhost:9092",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
			// other Kafka environment variables as needed...
		},
		// We wait for a specific log line that indicates Kafka has started.
		// Adjust if your Kafka version logs a different line:
		WaitingFor: wait.ForLog("Started NetworkTrafficServerConnector"),
	}

	// 2) Create and start the container
	kafkaC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerReq,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Kafka container")

	// Cleanup: ensure container is terminated at the end of the test
	defer func() {
		if terminateErr := kafkaC.Terminate(ctx); terminateErr != nil {
			log.Printf("Failed to terminate container: %v\n", terminateErr)
		}
	}()

	// 3) Extract the host and mapped port
	host, err := kafkaC.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	mappedPort, err := kafkaC.MappedPort(ctx, "9092/tcp")
	require.NoError(t, err, "Failed to get mapped Kafka port")

	// Build the actual broker address (e.g., "localhost:49154")
	brokers := []string{fmt.Sprintf("%s:%s", host, mappedPort.Port())}

	// 4) Create your KafkaTaskRepository
	//    (Make sure it’s updated to allow public access to the Producer,
	//     or else provide a getter function.)
	groupID := "test-consumer-group"
	inTopic := "test-tasks"
	outTopic := "test-results"

	repo, err := kafka.NewKafkaTaskRepository(brokers, groupID, inTopic, outTopic)
	require.NoError(t, err, "Failed to create KafkaTaskRepository")

	// 5) Start consuming tasks
	taskChan, errorChan := repo.ConsumeTasks()

	// 6) Produce a message to the inTopic
	testTask := &domain.Task{
		ID:      "test-task-1",
		Payload: "some test payload",
	}

	// If your repository exposes Producer as a public field, e.g. `repo.Producer`,
	// or a getter method like `repo.GetProducer()`, use that here:
	err = repo.GetProducer().SendMessage(inTopic, testTask.ID, testTask.Payload)
	require.NoError(t, err, "Failed to send message")

	// 7) Verify the consumer picks it up (up to 10 seconds)
	var receivedTask *domain.Task
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

consumptionLoop:
	for {
		select {
		case t := <-taskChan:
			if t != nil && t.ID == testTask.ID {
				receivedTask = t
				break consumptionLoop
			}
		case e := <-errorChan:
			require.Fail(t, fmt.Sprintf("Consumer error: %v", e))
		case <-timer.C:
			require.Fail(t, "Timed out waiting for consumer to receive the message")
		}
	}

	require.NotNil(t, receivedTask, "The received task should not be nil")
	require.Equal(t, testTask.ID, receivedTask.ID, "Task IDs must match")
	require.Equal(t, testTask.Payload, receivedTask.Payload, "Payload must match")

	// 8) Simulate processing: set the result and produce it to outTopic
	receivedTask.Result = "processed result"
	err = repo.ProduceResult(receivedTask)
	require.NoError(t, err, "Failed to produce result to outTopic")

	// 9) Create a second consumer to verify the message in outTopic
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	simpleConsumer, err := sarama.NewConsumer(brokers, config)
	require.NoError(t, err, "Failed to create simple consumer")
	defer func() {
		closeErr := simpleConsumer.Close()
		require.NoError(t, closeErr, "Failed to close consumer")
	}()

	partitionConsumer, err := simpleConsumer.ConsumePartition(outTopic, 0, sarama.OffsetOldest)
	require.NoError(t, err, "Failed to create partition consumer")
	defer func() {
		pcCloseErr := partitionConsumer.Close()
		require.NoError(t, pcCloseErr, "Failed to close partition consumer")
	}()

	foundOutput := false
	resultTimer := time.NewTimer(10 * time.Second)
	defer resultTimer.Stop()

resultLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			if string(msg.Key) == receivedTask.ID {
				foundOutput = true
				require.Contains(t, string(msg.Value), "processed result")
				break resultLoop
			}
		case <-resultTimer.C:
			break resultLoop
		}
	}

	require.True(t, foundOutput, "Did not receive processed result in outTopic")
}
