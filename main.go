package main

import (
	"flag"
	"log"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"time"
	"strings"
)

const (
	defaultKafkaAddress = "localhost:9092"
	defaultTopic        = "test"
	defaultGroup        = "kafka-tester"
)

var address string
var topic string
var group string

var asyncProducer *sarama.AsyncProducer
var consumer *cluster.Consumer

func main() {
	go sentTestMessage()
	consumeTestMessage()
	for {

	}
}

func init() {
	flag.StringVar(&address, "address", defaultKafkaAddress, "Address of kafka")
	flag.StringVar(&topic, "topic", defaultTopic, "Topic of kafka")
	flag.StringVar(&group, "group", defaultGroup, "Group of kafka Consumer")
	flag.Parse()
	log.Println("kafka address: ", address)
	log.Println("kafka topic: ", topic)
	log.Println("kafka group: ", group)
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
	for msg := range consumer.Messages() {
		log.Println("Reveive message success " + string(msg.Value))
		consumer.MarkOffset(msg, "")
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
				log.Println("Send message success" + string(messageByte))
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
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	(*asyncProducer).Input() <- msg
}
