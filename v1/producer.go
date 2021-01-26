package v1

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"strings"
	"time"
)

type ProducerConfig struct {
	Brokers      string `toml:"brokers"`
	Retries      int    `toml:"retries"`
	RetryBackoff string `toml:"retry_backoff"`
}

type SyncProducer struct {
	syncProducer sarama.SyncProducer
	conf         *ProducerConfig
}

func NewSyncProducer(config *ProducerConfig) (*SyncProducer, error) {
	duration, err := time.ParseDuration(config.RetryBackoff)
	if err != nil {
		duration = 1000 * time.Microsecond
	}

	if len(config.Brokers) == 0 {
		log.Panicf("no Kafka bootstrap brokers defined, please set the brokers conf")
	}

	retries := 3
	if config.Retries > 0 {
		retries = config.Retries
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V0_10_2_1
	saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal // 生产者等待leader响应、
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Retry.Max = retries
	saramaConfig.Producer.Retry.Backoff = duration
	if err = saramaConfig.Validate(); err != nil {
		log.Printf("[Kafka] producer config invalidate. config: %v. err: %v \n", *config, err)
		return nil, err
	}

	brokers := strings.Split(config.Brokers, ",")
	syncProducer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		log.Printf("[Kafka] error creating sync producer. err: %v \n", err)
		return nil, err
	}

	return &SyncProducer{
		syncProducer: syncProducer,
		conf:         config,
	}, nil
}

func (sp *SyncProducer) Send(ctx context.Context, topic string, msg []byte) (partition int32, offset int64, err error) {
	pm := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(msg),
		Timestamp: time.Now(),
	}

	partition, offset, err = sp.syncProducer.SendMessage(pm)
	log.Printf("[Kafka]消息发送至partition(%d) offset(%d) err(%v) \n", partition, offset, err)
	return
}

func (sp *SyncProducer) Close() {
	_ = sp.syncProducer.Close()
	return
}
