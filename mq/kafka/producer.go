package kafka

import (
	"github.com/IBM/sarama"
	"github.com/jifuy/commongo/loging"
	"strings"
)

// MQKafkaService Kafka消息队列
type MQKafkaService struct {
	Brokers string
	P       sarama.SyncProducer
	C       sarama.ConsumerGroup
}

func NewKafkaProducer(config Config) (MQKafkaService, func() error, error) {
	// 连接kafka
	client, err := sarama.NewSyncProducer(strings.Split(config.Brokers, ","), config.saram)
	if err != nil {
		return MQKafkaService{}, nil, err
	}
	return MQKafkaService{P: client},
		func() error {
			return client.Close()
		}, nil
}

// 同一分区只能被同消费组的一个消费组消费，可以发送到多个分区，相同消费组就能消费同一个topic了。
// Producer 生产者 当你发送消息到一个尚不存在的主题时，Kafka 默认会为该主题创建一个分区。这个分区被称为“分区0”。所以在初始阶段，发送到新主题的所有消息都会被发送到这个分区。也可以创建一个名为 “my-topic” 的主题，并指定了3个分区
func (m MQKafkaService) Producer(topic string, key, _ string, data []byte) error {
	var k sarama.Encoder
	if key == "" {
		k = nil
	} else {
		k = sarama.StringEncoder(key)
	}
	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Key = k //确保消息被发送到同一个分区保证消息顺序性 会hash到不同分区 负载均衡
	msg.Value = sarama.ByteEncoder(data)
	// 发送消息
	pid, offset, err := m.P.SendMessage(msg)
	if err != nil {
		loging.Log.Error("send msg failed, err:", err)
		return err
	}
	loging.Log.InfoF("pid:%v offset:%v\n", pid, offset)
	return nil
}
