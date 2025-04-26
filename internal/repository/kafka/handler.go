package kafka_repository

import (
	"encoding/json"
	"fmt"
	api "github.com/5krotov/task-resolver-pkg/api/v1"
	entity "github.com/5krotov/task-resolver-pkg/entity/v1"
	"github.com/IBM/sarama"
	"log"
	"time"
	"worker-service/internal/future"
)

type Handler struct {
	StatusTopic string
	Producer    sarama.SyncProducer
	Tasks       chan *future.TaskFuture
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
messageLoop:
	for message := range claim.Messages() {
		var task api.StartTaskRequest
		err := json.Unmarshal(message.Value, &task)
		if err != nil {
			log.Printf("unmarshal task failed: %v\n", err)
			session.MarkMessage(message, "")
			session.Commit()
			continue messageLoop
		}

		log.Printf("getted task %v\n", task.Id)

		err = h.UpdateTaskStatus(api.UpdateStatusRequest{Id: task.Id, Status: entity.Status{Status: entity.STATUS_PENDING, Timestamp: time.Now()}})
		if err != nil {
			log.Printf("send task %v status %v failed: %v\n", task.Id, entity.STATUS_PENDING, err)
			session.MarkMessage(message, "")
			session.Commit()
			continue messageLoop
		}

		taskFuture := future.NewTaskFuture(task)
		h.Tasks <- taskFuture

	futureLoop:
		for {
			select {
			case res, ok := <-taskFuture.Result:
				if !ok {
					break futureLoop
				}
				updateStatusErr := h.UpdateTaskStatus(res)
				if updateStatusErr != nil {
					log.Printf("send task %v status %v failed: %v\n", task.Id, res.Status.Status, updateStatusErr)
					session.MarkMessage(message, "")
					session.Commit()
					continue messageLoop
				}
				continue futureLoop
			case futureErr := <-taskFuture.Err:
				log.Printf("future work task %v error: %v", task.Id, futureErr)
				session.MarkMessage(message, "")
				session.Commit()
				continue messageLoop
			}
		}

		session.MarkMessage(message, "")
		session.Commit()

	}

	return nil
}

func (h *Handler) UpdateTaskStatus(request api.UpdateStatusRequest) error {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal update status failed: %v", err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     h.StatusTopic,
		Value:     sarama.StringEncoder(requestJSON),
		Partition: -1,
	}

	_, _, err = h.Producer.SendMessage(msg)

	if err != nil {
		return fmt.Errorf("send message failed: %v", err)
	}
	return nil
}
