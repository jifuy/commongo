package rocket

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/jifuy/commongo/loging"
	"github.com/jifuy/commongo/mq/kafka"
	"github.com/pkg/errors"
	"strings"
)

func NewRocketCustomer(config Config) (MQRocket, func() error, error) {
	options := make([]consumer.Option, 0)
	addr, err := primitive.NewNamesrvAddr(strings.Split(config.Brokers, ",")...)
	if err != nil {
		return MQRocket{}, nil, err
	}
	options = append(options, consumer.WithNameServer(addr))

	if config.Group == "" {
		return MQRocket{}, nil, errors.Wrapf(err, "消费组不能为空 %s", config.Group)
	}
	options = append(options, consumer.WithGroupName(config.Group))

	if config.PassWord != "" {
		options = append(options, consumer.WithCredentials(
			primitive.Credentials{
				AccessKey: config.UserName,
				SecretKey: config.PassWord,
			}))
	}
	if config.MaxSpan == 0 {
		config.MaxSpan = 2000
	}
	options = append(options, consumer.WithConsumeConcurrentlyMaxSpan(config.MaxSpan))

	if config.InstanceName == "" {
		config.InstanceName = kafka.NewUUIDStr()
	}
	options = append(options, consumer.WithInstance(config.InstanceName))

	if config.MsgMod == consumer.BroadCasting.String() {
		options = append(options, consumer.WithConsumerModel(consumer.BroadCasting))
	}

	// 消费者连接
	com, err := rocketmq.NewPushConsumer(options...)
	if err != nil {
		loging.Debug("消费者连接失败！")
		return MQRocket{}, nil, err
	}
	err = com.Start()
	if err != nil {
		loging.Debug("消费者启动失败！")
		return MQRocket{}, nil, err
	}
	return MQRocket{C: com},
		func() error {
			return com.Shutdown()
		}, nil
}

func (r MQRocket) Consumer(topic, _, _ string, f func(b []byte) bool) (func() error, error) {
	err := r.C.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, i := range ext {
			if ok := f(i.Body); !ok {
				loging.Debug("消费失败")
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	return func() error {
		return nil
	}, err
}
