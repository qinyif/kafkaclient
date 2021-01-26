package kafkaclient

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-light/metadata"
	"log"
	"strings"
	"sync"
	"time"
)

type ConsumerConfig struct {
	Brokers            string
	Group              string
	Topics             string
	OffsetReset        string // latest, earliest, none
	Assignor           string // sticky, roundrobin, range
	AutoCommit         bool   `toml:"auto_commit"`
	AutoCommitInterval string `toml:"auto_commit_interval"`
	FailTries          int
}

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

type Consumer struct {
	client sarama.ConsumerGroup
	conf   *ConsumerConfig
	ctx    context.Context
	cancel context.CancelFunc
	close  bool
}

func NewConsumer(conf *ConsumerConfig) (*Consumer, error) {
	if len(conf.Brokers) == 0 {
		log.Panicf("no Kafka bootstrap brokers defined, please set the brokers config")
	}

	if len(conf.Topics) == 0 {
		log.Panicf("no topics given to be consumed, please set the topics config")
	}

	if len(conf.Group) == 0 {
		log.Panicf("no Kafka consumer group defined, please set the group config")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V0_10_2_1
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = conf.AutoCommit
	if conf.AutoCommit && len(conf.AutoCommitInterval) != 0 {
		duration, err := time.ParseDuration(conf.AutoCommitInterval)
		if err != nil {
			log.Panicf("no Kafka auto_commit_interval defined, please set the auto_commit_interval config")
		}
		saramaConfig.Consumer.Offsets.AutoCommit.Interval = duration
	} else {
		saramaConfig.Consumer.Offsets.AutoCommit.Interval = 5000 * time.Millisecond
	}

	switch conf.Assignor {
	case "sticky":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		panic(fmt.Sprintf("Unrecognized consumer group partition assignor: %s", conf.Assignor))
	}

	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	if conf.OffsetReset == "earliest" {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	if err := saramaConfig.Validate(); err != nil {
		return nil, err
	}

	brokers := strings.Split(conf.Brokers, ",")
	consumerGroup, err := sarama.NewConsumerGroup(brokers, conf.Group, saramaConfig)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		conf:   conf,
		ctx:    ctx,
		cancel: cancel,
		client: consumerGroup,
	}

	return consumer, nil
}

func (c *Consumer) Receive(callback MsgProcess) {
	client := c.client

	consumer := kafkaConsumerHandler{
		autoAck:  c.conf.AutoCommit,
		ready:    make(chan bool),
		callback: callback,

		tries: c.conf.FailTries,
		delayFunc: func(tries int) time.Duration {
			return 500 * time.Millisecond
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(c.ctx, strings.Split(c.conf.Topics, ","), &consumer); err != nil {
				//log.Panicf("Error from consumer: %v", err)
				log.Printf("Error from consumer: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			if err := c.ctx.Err(); err != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	wg.Wait()
	if err := client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func (c *Consumer) Close() (err error) {
	c.close = true
	c.cancel()
	if c.client != nil {
		err = c.client.Close()
	}
	return
}

type MsgProcess func(ctx context.Context, msg *sarama.ConsumerMessage) (err error)

// Consumer represents a Sarama consumer group consumer
type kafkaConsumerHandler struct {
	ready   chan bool
	autoAck bool

	callback MsgProcess

	tries     int
	delayFunc DelayFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (k *kafkaConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(k.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (k *kafkaConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (k *kafkaConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		headers := message.Headers

		md := make(map[string]interface{})
		for _, header := range headers {
			md[string(header.Key)] = string(header.Value)
		}
		ctx := metadata.NewContext(context.Background(), metadata.New(md))

		for i := 0; i < k.tries; i++ {
			if i != 0 {
				time.Sleep(k.delayFunc(i))
			}
			err := k.callback(ctx, message)
			if err != nil {
				continue
			}
			break
		}
		session.MarkMessage(message, "")
		if !k.autoAck {
			session.Commit()
		}
	}
	return nil
}
