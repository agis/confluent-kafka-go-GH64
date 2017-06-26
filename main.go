package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var wg sync.WaitGroup
var closeChan = make(chan struct{})

func main() {
	for i := 0; i <= 3; i++ {
		wg.Add(1)
		go consRun(closeChan)
	}

	time.Sleep(5 * time.Second)
	fmt.Println("Signalling consumers to close...")
	close(closeChan)
	wg.Wait()
	fmt.Println("All consumers closed successfully. Bye")
}

func consRun(done chan struct{}) {
	defer wg.Done()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               "kafka-a.example.com,kafka-b.example.com,kafka-c.example.com",
		"group.id":                        "foo",
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.auto.commit":              false,
		"auto.offset.reset":               "latest",
		"debug":                           "cgrp",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	topic := "test-foo"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal(err)
	}

Loop:
	for {
		select {
		case <-done:
			break Loop
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%v\n", e)
				c.Unassign()
			case *kafka.Message:
				fmt.Printf("Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))

				tp := kafka.TopicPartition{
					Topic:     e.TopicPartition.Topic,
					Partition: 0,
					Offset:    e.TopicPartition.Offset + 1,
				}
				_, err = c.CommitOffsets([]kafka.TopicPartition{tp})
				if err != nil {
					fmt.Print(err)
				}
			case kafka.PartitionEOF:
				fmt.Printf("Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v\n", e)
				break Loop
			default:
				fmt.Printf("unhandled event: %s", e)
			}
		}
	}

	fmt.Printf("I'm %s and I'm calling Close()...\n", c)
	c.Close()
	fmt.Println("Bye from ", c)
}
