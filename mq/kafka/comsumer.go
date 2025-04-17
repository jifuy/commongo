package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/jifuy/commongo/loging"
	"strings"
	"time"
)

func NewKafkaCustomer(config Config) (MQKafkaService, func() error, error) {
	if config.Group == "" {
		// 1个partition只能被同group的一个consumer消费，随机创建group，多个consumer可以消费同一个topic ,不同为空。随机多个消费组都消费
		config.Group = NewUUIDStr()
	}
	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(config.Brokers, ","), config.Group, config.saram) //同一个消费组话只能被一个人消费 ,这种模式group不能为空
	if err != nil {
		return MQKafkaService{}, nil, err
	}
	return MQKafkaService{C: consumerGroup, Config: config},
		func() error {
			return consumerGroup.Close()
		}, nil
}

func SetLogger(logger sarama.StdLogger) {
	sarama.Logger = logger
}

// Consumer 消费者组 组不相同时都可以消费到,启动吧没消费的也消费了
func (m MQKafkaService) Consumer(topic, _, _ string, f func(b []byte) bool) (func() error, error) {

	ctx, cancel := context.WithCancel(context.Background())
	consumer := Consumer{cb: f}

	go func() {
		for {
			select {
			case <-ctx.Done():
				loging.Info("Error from consumer ctx done")
				return
			default:
				if err1 := m.C.Consume(ctx, []string{topic}, &consumer); err1 != nil {
					if errors.Is(err1, sarama.ErrClosedClient) || errors.Is(err1, sarama.ErrOutOfBrokers) || errors.Is(err1, sarama.ErrNotConnected) {
						loging.Errorf("Kafka consumer error: %v, attempting to reconnect...", err1)
						for {
							time.Sleep(5 * time.Second) // 等待5秒后重试
							newConsumerGroup, err := sarama.NewConsumerGroup(strings.Split(m.Config.Brokers, ","), m.Config.Group, m.Config.saram)
							if err == nil {
								m.C = newConsumerGroup
								break
							}
							loging.Errorf("Failed to reconnect to Kafka, retrying...: %v", err)
						}
						continue
					}
					loging.Errorf("Error from consumer: %v", err1)
				}
				// check if context was canceled, signaling that the consumer should stop
				if ctx.Err() != nil {
					loging.Errorf("Error from consumer ctx: %v", ctx.Err())
					return
				}
			}
		}
	}()

	return func() error {
		cancel()
		return m.C.Close()
	}, nil
}

type Consumer struct {
	cb func(b []byte) bool
}

func (consumer *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Cleanup(s sarama.ConsumerGroupSession) error {
	s.Commit()
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		//loging.Infof("Partition:%d, Offset:%d, key:%s", message.Partition, message.Offset, string(message.Key))
		if !consumer.cb(message.Value) {
			// 如果回调函数返回 false，不确认消息
			loging.Warnf("Callback returned false, not marking message as processed. Partition:%d, Offset:%d, key:%s, value:%s", message.Partition, message.Offset, string(message.Key), string(message.Value))
			continue
		}
		session.MarkMessage(message, "")
	}

	return nil
}
