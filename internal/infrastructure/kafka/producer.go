package kafka

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type Producer struct {
	syncProducer sarama.SyncProducer
}

func NewProducer(brokerAddr string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Version = sarama.V2_5_0_0

	sp, err := sarama.NewSyncProducer([]string{brokerAddr}, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{syncProducer: sp}, nil
}

func (p *Producer) Close() error {
	return p.syncProducer.Close()
}

func (p *Producer) SendMessage(topic, key string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(bytes),
	}
	partition, offset, err := p.syncProducer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("[Producer] -> topic=%s, partition=%d, offset=%d", topic, partition, offset)
	return nil
}
