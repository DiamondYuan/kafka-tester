package main

import (
"fmt"
"strings"
"github.com/Shopify/sarama"
"os"
"log"
	"time"
)

var asyncProducer *sarama.AsyncProducer
var topic string

func main() {
	address := os.Getenv("KAFKA_ADDRESS")
	topic = os.Getenv("KAFKA_TOPIC")
	if topic == "" || address == "" {
		log.Fatalf("缺少kafka配置")
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	p, err := sarama.NewAsyncProducer(strings.Split(address, ","), config)
	if err != nil {
		log.Printf("kafka producer创建失败 address:%s  topic:%s", address, topic)
		log.Fatalf("kafka连接失败")
	} else {
		log.Printf("kafka producer创建成功 address:%s  topic:%s", address, topic)
	}
	asyncProducer = &p
	for {
		time.Sleep(time.Second)
		log.Printf(time.Unix(time.Now().Unix(), 0).String())
		sendMessage(time.Unix(time.Now().Unix(), 0).String())
	}

}


func sendMessage(message string) {
	if asyncProducer == nil {
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	p := *asyncProducer
	go func(p sarama.AsyncProducer) {
		for {
			select {
			case err := <-p.Errors():
				if err != nil {
					fmt.Println("kafka error")
				}
			case <-p.Successes():
			}
		}
	}(p)
	p.Input() <- msg
}

