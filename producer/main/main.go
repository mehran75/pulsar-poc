package main

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
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

	createProducer := func(name string, wg *sync.WaitGroup) {

		defer wg.Done()
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic:       name,
			SendTimeout: 30 * time.Second,
		})
		handlerErr(err)

		id, err := producer.Send(context.Background(), &pulsar.ProducerMessage{Payload: []byte("Hello this is POC " + name)})
		handlerErr(err)
		logrus.WithField("Id", id).WithField("producer", name).Info("Message sent")
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go createProducer("P-1", &wg)
	go createProducer("P-2", &wg)

	wg.Wait()

}
