package main

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"sync"
)

func handlerErr(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	handlerErr(err)

	createConsumer := func(name string, wg *sync.WaitGroup) {
		defer wg.Done()

		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            name,
			SubscriptionName: name,
			Name:             name,
		})
		handlerErr(err)

		for {
			message, err := consumer.Receive(context.Background())
			handlerErr(err)
			logrus.WithField("Name", name).WithField("Message", string(message.Payload())).Info("message received")
		}
	}
	wg := sync.WaitGroup{}

	wg.Add(2)
	go createConsumer("P-1", &wg)
	go createConsumer("P-2", &wg)
	wg.Wait()

}
