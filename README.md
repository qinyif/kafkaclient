# kafka-client
kafka client

##Install:

	go get github.com/go-light/kafka-client/v1

## Consumer


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

## Producer

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

	
	_, _, err = syncProducer.Send(context.Background(), topic, msg)
	if err != nil {
		t.Error(err)
		return
	}
	