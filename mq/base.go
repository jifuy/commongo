package mq

import (
	"fmt"
	"github.com/jifuy/commongo/mq/kafka"
	"github.com/jifuy/commongo/mq/rocket"
)

// 也长连接
type ICustomer interface {
	Consumer(topic, group, routekey string, f func(b []byte) bool) (func() error, error)
}

// 不需要每次发送消息都重新连接和关闭连接因为频繁的连接和断开会增加网络开销和延迟。提高效率
type IProducer interface {
	Producer(topic string, topickey string, routekey string, data []byte) error
}

type MqCfg struct {
	MqType   string
	Kafka    KafkaCfg
	RabbitMq RabbitCfg
	RocketMq RocketCfg
}

type KafkaCfg struct {
	Version string //kafka 版本
	Brokers string //必填

	Group    string //消费者时 填写 消费组
	Assignor string //消费者时 负载均衡策略  sticky  roundrobin range

	SaslEnable bool   //有认证为true
	Algorithm  string //加密形式 sha512 sha256 plain oauthbearer gssapi
	UserName   string
	PassWord   string
	//gssapi配置
	Realm              string
	ServiceName        string
	KeyTabPath         string
	KerberosConfigPath string
	// 配置TLS（如果需要）
	UseTLS    bool
	VerifySSL bool
	CertFile  string
	KeyFile   string
	CaFile    string
}

type RabbitCfg struct {
	Url string // amqp://guest:guest@192.168.143.112:5672/
}

type RocketCfg struct {
	Brokers  string
	UserName string
	PassWord string
	Group    string //消费者时 填写

	InstanceName string
	MsgMod       string //消息模式  BroadCasting/Clustering
	MaxSpan      int
}

// NewProducerMQ 实例化消息队列对象
func NewProducerMQ(mqChg MqCfg) (IProducer, func() error, error) {
	switch mqChg.MqType { // mq 设置的类型
	case "kafka":
		var config = kafka.Config{
			Version: mqChg.Kafka.Version,
			Brokers: mqChg.Kafka.Brokers,

			SaslEnable: mqChg.Kafka.SaslEnable,
			Algorithm:  mqChg.Kafka.Algorithm,
			UserName:   mqChg.Kafka.UserName,
			PassWord:   mqChg.Kafka.PassWord,

			Realm:              mqChg.Kafka.Realm,
			ServiceName:        mqChg.Kafka.ServiceName,
			KeyTabPath:         mqChg.Kafka.KeyTabPath,
			KerberosConfigPath: mqChg.Kafka.KerberosConfigPath,

			UseTLS:    mqChg.Kafka.UseTLS,
			VerifySSL: mqChg.Kafka.VerifySSL,
			CertFile:  mqChg.Kafka.CertFile,
			KeyFile:   mqChg.Kafka.KeyFile,
			CaFile:    mqChg.Kafka.CaFile,
		}
		config.InitSarama()
		return kafka.NewKafkaProducer(config)
	case "rocket":
		var config = rocket.Config{
			Brokers:      mqChg.RocketMq.Brokers,
			UserName:     mqChg.RocketMq.UserName,
			PassWord:     mqChg.RocketMq.PassWord,
			InstanceName: mqChg.RocketMq.InstanceName,
		}
		return rocket.NewRocketProducer(config)
	default:
		return nil, nil, fmt.Errorf("mq type error %s", mqChg.MqType)
	}
}

// NewMQ 实例化消息队列对象
func NewConsumerMQ(mqChg MqCfg) (ICustomer, func() error, error) {
	switch mqChg.MqType {
	case "kafka":
		var config = kafka.Config{
			Brokers: mqChg.Kafka.Brokers,
			Group:   mqChg.Kafka.Group,

			SaslEnable: mqChg.Kafka.SaslEnable,
			Algorithm:  mqChg.Kafka.Algorithm,
			UserName:   mqChg.Kafka.UserName,
			PassWord:   mqChg.Kafka.PassWord,
		}
		config.InitSarama()
		return kafka.NewKafkaCustomer(config)
	case "rocket":
		var config = rocket.Config{
			Brokers:      mqChg.RocketMq.Brokers,
			Group:        mqChg.RocketMq.Group,
			UserName:     mqChg.RocketMq.UserName,
			PassWord:     mqChg.RocketMq.PassWord,
			InstanceName: mqChg.RocketMq.InstanceName,
			MsgMod:       mqChg.RocketMq.MsgMod,
			MaxSpan:      mqChg.RocketMq.MaxSpan,
		}
		return rocket.NewRocketCustomer(config)
	default:
		return nil, nil, nil
	}
}
