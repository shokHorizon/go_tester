package repos

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/shokHorizon/go_tester/internals/entity"
)

type KafkaRepository struct {
	reader *kafka.Reader
}

func NewKafkaRepository(brokers []string, topic string, groupID string) (*KafkaRepository, error) {
	config := kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	}
	reader := kafka.NewReader(config)
	return &KafkaRepository{reader}, nil
}

func (r *KafkaRepository) ReadSolution(ctx context.Context) (*entity.Solution, error) {
	for {
		msg, err := r.reader.ReadMessage(ctx)
		if err != nil {
			return nil, err
		}
		var sol entity.Solution
		err = json.Unmarshal(msg.Value, &sol)
		if err != nil {
			fmt.Printf("Error unmarshalling Solution: %v\n", err)
			continue
		}
		return &sol, nil
	}
}
