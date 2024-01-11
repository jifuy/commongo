package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	logging "github.com/jifuy/commongo/loging"
	"strings"
)

func NewKafkaCustomer(config Config) (MQKafkaService, func() error, error) {
	if config.Group == "" {
		// 1个partition只能被同group的一个consumer消费，随机创建group，多个consumer可以消费同一个topic ,不同为空。随机多个消费组都消费
		config.Group = NewUUIDStr()
	}
	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(config.Brokers, ","), config.Group, config.saram) //同一个消费组话只能被一个人消费 ,这种模式group不能为空
	// 连接kafka
	if err != nil {
		return MQKafkaService{}, nil, err
	}
	return MQKafkaService{C: consumerGroup},
		func() error {
			return consumerGroup.Close()
		}, nil
}

// Consumer 消费者组 组不相同时都可以消费到,启动吧没消费的也消费了
func (m MQKafkaService) Consumer(topic, _, _ string, f func(b []byte) bool) (func() error, error) {

	ctx, cancel := context.WithCancel(context.Background())
	consumer := Consumer{cb: f}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err1 := m.C.Consume(ctx, []string{topic}, &consumer); err1 != nil {
					fmt.Println("你大爷Error from consumer: %+w", err1)
				}
				// check if context was canceled, signaling that the consumer should stop
				if ctx.Err() != nil {
					fmt.Println("一遍遍", ctx.Err())
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
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.cb(message.Value)
		logging.DebugF("Partition:%d, Offset:%d, key:%s, value:%s\n", message.Partition, message.Offset, string(message.Key), string(message.Value))
		session.MarkMessage(message, "")
	}

	return nil
}
