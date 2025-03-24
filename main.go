package main

import (
	"github.com/jifuy/commongo/mq"
)

func main() {
	var mqCfg = mq.MqCfg{MqType: "kafka", Kafka: mq.KafkaCfg{Brokers: "127.0.0.1:9092", Group: "group1"}}
	var cus, ch, _ = mq.NewConsumerMQ(mqCfg)
	defer ch()
	ch2, _ := cus.Consumer("unios-alarm-std", "", "", func(b []byte) bool {
		//time.Sleep(time.Second * 100)
		//fmt.Println("---", string(b))
		return true
	})
	defer ch2()

	select {}
}
