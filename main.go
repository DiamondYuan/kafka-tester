package main

import (
	"fmt"
	"strings"
	"github.com/Shopify/sarama"
	"os"
	"time"
	"github.com/bsm/sarama-cluster"
)

var asyncProducer *sarama.AsyncProducer
var topic string

func main() {
	//os.Setenv("MODE","all")
	//os.Setenv("KAFKA_ADDRESS","localhost:9092")
	//os.Setenv("KAFKA_TOPIC","KAFKA_TOPIC")
	mode := os.Getenv("MODE")
	switch mode {
	case "all":
		startProducer()
		startConsumer()
	case "producer":
		startProducer()
		go sentTestMessage()
		sentTestMessage()
	case "consumer":
		startConsumer()
	default:
		startProducer()
		go sentTestMessage()
		startConsumer()
	}
}

func startConsumer() {
	groupID := "group-1"
	address := os.Getenv("KAFKA_ADDRESS")
	topic = os.Getenv("KAFKA_TOPIC")
	if topic == "" || address == "" {
		fmt.Printf("缺少kafka配置")
		return
	}
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	c, err := cluster.NewConsumer(strings.Split(address, ","), groupID, strings.Split(topic, ","), config)
	if err != nil {
		return
	}
	defer c.Close()
	go func(c *cluster.Consumer) {
		errors := c.Errors()
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				fmt.Println(err.Error())
			case <-noti:
			}
		}
	}(c)
	for msg := range c.Messages() {
		fmt.Println("receive " + string(msg.Value))
		c.MarkOffset(msg, "")
	}
}

func sentTestMessage() {
	for {
		time.Sleep(time.Second)
		fmt.Println("send " + time.Unix(time.Now().Unix(), 0).String())
		sendMessage(time.Unix(time.Now().Unix(), 0).String())
	}
}

func startProducer() {
	address := os.Getenv("KAFKA_ADDRESS")
	topic = os.Getenv("KAFKA_TOPIC")
	if topic == "" || address == "" {
		fmt.Println("缺少kafka配置")
		return
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	p, err := sarama.NewAsyncProducer(strings.Split(address, ","), config)
	if err != nil {
		fmt.Println("kafka producer创建失败 address:%s  topic:%s", address, topic)
		return
	} else {
		fmt.Println("kafka producer创建成功 address:%s  topic:%s", address, topic)
	}
	asyncProducer = &p

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
