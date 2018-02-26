package main

import (
	"flag"
	"log"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"time"
	"strings"
	"fmt"
)

const (
	defaultKafkaAddress = "localhost:9092"
	defaultTopic        = "test"
	defaultGroup        = "kafka-tester"
	defaultMode         = "all"
)

var address string
var topic string
var group string
var mode string
var messageIndex int

var asyncProducer *sarama.AsyncProducer
var consumer *cluster.Consumer

func main() {
	messageIndex = 0;
	switch mode {
	case "consumer":
		log.Println("Start consumer")
		consumeTestMessage()
	case "producer":
		log.Println("Start producer")
		go sentTestMessage()

	default:
		log.Println("Start producer and consumer")
		go sentTestMessage()
		consumeTestMessage()
	}
	for {

	}
}

func init() {
	flag.StringVar(&address, "address", defaultKafkaAddress, "Address of kafka")
	flag.StringVar(&topic, "topic", defaultTopic, "Topic of kafka")
	flag.StringVar(&group, "group", defaultGroup, "Group of kafka Consumer")
	flag.StringVar(&mode, "mode", defaultMode, "kafka tester mode producer or consumer")
	flag.Parse()
	log.Println("kafka address: ", address)
	log.Println("kafka topic: ", topic)
	log.Println("kafka group: ", group)
	log.Println("kafka tester mode: ", group)
	{
		err := initConsumer()
		if err != nil {
			log.Fatal(err)
		}

	}
	{
		err := initProducer()
		if err != nil {
			log.Fatal(err)
		}
	}
}
func initConsumer() error {
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	c, err := cluster.NewConsumer(strings.Split(address, ","), group, strings.Split(topic, ","), config)
	if err != nil {
		return err
	}
	consumer = c
	return nil
}

func initProducer() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	p, err := sarama.NewAsyncProducer(strings.Split(address, ","), config)
	if err != nil {
		return err
	}
	asyncProducer = &p
	return nil
}

func consumeTestMessage() {
	go func(c *cluster.Consumer) {
		errors := c.Errors()
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				log.Println(err.Error())
			case <-noti:
			}
		}
	}(consumer)
	for {
		select {
		case message := <-consumer.Messages():
			log.Printf("Reveive message success %s offset:%d partition:%d", string(message.Value), message.Offset, message.Partition)
			consumer.MarkOffset(message, "")
		}
	}
}

func sentTestMessage() {
	go func(p sarama.AsyncProducer) {
		for {
			select {
			case err := <-p.Errors():
				if err != nil {
					log.Println(err)
				}
			case msg := <-p.Successes():
				messageByte, _ := msg.Value.Encode()
				log.Printf("Send message success %s offset:%d partition:%d", string(messageByte), msg.Offset, msg.Partition)
			}
		}
	}(*asyncProducer)
	for {
		time.Sleep(time.Second)
		sendMessage(time.Unix(time.Now().Unix(), 0).String())
	}
}

func sendMessage(message string) {
	if asyncProducer == nil {
		log.Fatal("asyncProducer is nil")
	}
	message = fmt.Sprintf("%d   %s", messageIndex, message)
	messageIndex++
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	(*asyncProducer).Input() <- msg
}
