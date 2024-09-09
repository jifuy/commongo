package mq

import (
	"fmt"
	"github.com/jifuy/commongo/loging"
	"testing"
	"time"
)

// rocket生产者
func TestNewMQ2(t *testing.T) {
	var mqCfg = MqCfg{MqType: "rocket", RocketMq: RocketCfg{Brokers: "127.0.0.1:9876"}}
	var mq1, ch, _ = NewProducerMQ(mqCfg)
	defer ch()
	for i := 0; i < 3; i++ {
		err := mq1.Producer("unios-alarm-notify", "_", "", []byte("Msg+"+fmt.Sprint(i)))
		if err != nil {
			fmt.Println("记录消息后续发送")
		}
	}
	t.Log("hello world")
}

// rocket消费者，组不能相同
func TestMQCus3(t *testing.T) {
	var mqCfg = MqCfg{MqType: "rocket", RocketMq: RocketCfg{Brokers: "127.0.0.1:9876", Group: "xiaofeizhu"}}

	var cus, ch, _ = NewConsumerMQ(mqCfg)
	defer ch()
	ch2, _ := cus.Consumer("unios-alarm-notify", "", "", func(b []byte) bool {
		fmt.Println("---", string(b))
		return true
	})
	defer ch2()

	time.Sleep(5 * time.Second)
	fmt.Println("------------------------------------------------------")
	mqCfg.RocketMq.Group = "xiaofeizhu3"
	var cus_, ch_, _ = NewConsumerMQ(mqCfg)
	defer ch_()
	ch2_, _ := cus_.Consumer("unios-alarm-notify", "", "", func(b []byte) bool {
		loging.Log.Debug("记录消息后续发送", "---++", string(b))
		return true
	})
	defer ch2_()

	time.Sleep(300 * time.Second)
	t.Log("hello world")

}

func TestNewMQ4(t *testing.T) {
	//var mqCfg = MqCfg{MqType: "kafka", Kafka: KafkaCfg{Brokers: "127.0.0.1:9092,127.0.0.1:9093"}}
	var mqCfg = MqCfg{MqType: "kafka", Kafka: KafkaCfg{Brokers: "192.168.5.189:9094", SaslEnable: true, Group: "xiaofeizhu", Version: "2.4.0", UserName: "kafka", PassWord: "c!E0ULhH", Algorithm: "plain"}}
	var mq1, ch, err = NewProducerMQ(mqCfg)
	if err != nil {
		fmt.Println("1111111err--------", err)
		return
	}
	time.Sleep(3 * time.Second)
	defer ch()
	for i := 0; i < 3; i++ {
		err := mq1.Producer("my-topic", "xsq", "", []byte("Msg+"+fmt.Sprint(i)))
		if err != nil {

		}
		loging.Log.Debug("记录消息后续发送", err)
		time.Sleep(1 * time.Second)
	}
	t.Log("hello world")
}

// rocket消费者，组不能相同
func TestMQCus5(t *testing.T) {
	var mqCfg = MqCfg{MqType: "kafka", Kafka: KafkaCfg{Brokers: "192.168.5.189:9094", SaslEnable: true, Oldest: true, Group: "xiaofeizhu1", Version: "2.4.0", UserName: "kafka", PassWord: "c!E0ULhH", Algorithm: "plain"}}

	var cus, ch, _ = NewConsumerMQ(mqCfg)
	defer ch()
	ch2, _ := cus.Consumer("my-topic", "", "", func(b []byte) bool {
		fmt.Println("---", string(b))
		return true
	})
	defer ch2()

	time.Sleep(5 * time.Second)
	fmt.Println("------------------------------------------------------")
	//mqCfg.RocketMq.Group = "xiaofeizhu3"
	//var cus_, ch_, _ = NewConsumerMQ(mqCfg)
	//defer ch_()
	//ch2_, _ := cus_.Consumer("my-topic", "", "", func(b []byte) bool {
	//	fmt.Println("---++", string(b))
	//	return true
	//})
	//defer ch2_()

	time.Sleep(300 * time.Second)
	t.Log("hello world")

}
