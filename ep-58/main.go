package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("Kafka Producer using Golang")

	writer := kafka.Writer{
		Addr:  kafka.TCP("0.0.0.0:9092"),
		Topic: "goscreencasts",
	}
	defer writer.Close()

	for i := 0; ; i++ {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i+1)),
			Value: []byte(fmt.Sprintf("Message : %d", i+1)),
		}

		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Printf("Message %d sent \n", i+1)
		time.Sleep(1 * time.Second)
	}

}
