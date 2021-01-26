package kafkaclient

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
)

func TestNewConsumer(t *testing.T) {
	config := &ConsumerConfig{
		Brokers: "127.0.0.1:9092", // "127.0.0.1:9092,127.0.0.2:9092"
		Group: "test_group",
		Topics: "test_kafka_producer", // "test_kafka_producer,test_kafka_producer1"
		OffsetReset: "latest", // "latest" , "earliest"
		Assignor: "roundrobin", // "sticky", "roundrobin" , "range"
		AutoCommit: false,
		AutoCommitInterval: "5000ms",
		FailTries: 4, // 消息消费失败重试次数
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
		return
	}

	consumer.Receive(func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		fmt.Println(string(msg.Value))
		fmt.Println("fail_tries")
		err = fmt.Errorf("fail_tries")
		return err
	})
}


