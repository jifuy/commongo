package rocket

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/jifuy/commongo/loging"
	"github.com/jifuy/commongo/mq/kafka"
	"github.com/pkg/errors"
	"strings"
)

type Config struct {
	Brokers  string
	UserName string
	PassWord string
	Group    string //消费组

	InstanceName string
	MsgMod       string //消息模式  BroadCasting/Clustering
	MaxSpan      int
}

type MQRocket struct {
	C rocketmq.PushConsumer
	P rocketmq.Producer
}

func NewRocketProducer(config Config) (MQRocket, func() error, error) {
	ops := make([]producer.Option, 0)
	ops = append(ops, producer.WithNsResolver(primitive.NewPassthroughResolver(strings.Split(config.Brokers, ","))))
	ops = append(ops, producer.WithRetry(2))
	ops = append(ops, producer.WithQueueSelector(producer.NewHashQueueSelector()))

	if config.InstanceName == "" {
		config.InstanceName = kafka.NewUUIDStr()
	}
	ops = append(ops, producer.WithInstanceName(config.InstanceName))

	if config.PassWord != "" {
		ops = append(ops, producer.WithCredentials(primitive.Credentials{
			AccessKey: config.UserName,
			SecretKey: config.PassWord,
		}))
	}
	// 连接kafka
	pclient, err := rocketmq.NewProducer(ops...)
	if err != nil {
		return MQRocket{}, nil, err
	}
	if err = pclient.Start(); err != nil {
		return MQRocket{}, nil, errors.Wrapf(err, "start rocket producer failed, broker %s", config.Brokers)
	}
	return MQRocket{P: pclient},
		func() error {
			return pclient.Shutdown()
		}, nil
}

// Producer 生产者
func (m MQRocket) Producer(topic string, key, _ string, data []byte) error {
	msg := primitive.NewMessage(topic, data)
	msg.WithProperty(primitive.PropertyShardingKey, key)
	res, err := m.P.SendSync(context.TODO(), msg)
	if err != nil {
		return errors.Wrapf(err, "send to ctg-mq failed")
	}
	loging.DebugF("send message success: result=%s\n", res.String())
	return nil
}
