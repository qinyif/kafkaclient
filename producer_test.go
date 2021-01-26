package kafkaclient

import (
	"context"
	"encoding/json"
	"testing"
)

func TestNewSyncProducer(t *testing.T) {
	config := &ProducerConfig{
		Brokers: "127.0.0.1:9092",
		Retries: 3,
	}

	syncProducer, err := NewSyncProducer(config)
	if err != nil {
		t.Error(err)
		return
	}

	topic := "test_kafka_producer"
	msg, err := json.Marshal(map[string]string{"foo":"bar"})
	if err !=nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		_, _, err = syncProducer.Send(context.Background(), topic, msg)
		if err != nil {
			t.Error(err)
			return
		}
	}

}
