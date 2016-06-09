package main

import (
	"github.com/AhmetS/gostalk"
	"log"
	"time"
)

func main() {
	consumer, err := gostalk.Connect("127.0.0.1:11300")
	handleError(err)
	defer consumer.Disconnect()

	_, err = consumer.Watch("1m")
	handleError(err)
	consumer.Ignore("default")
	handleError(err)

	producer, err := gostalk.Connect("127.0.0.1:11300")
	handleError(err)
	defer producer.Disconnect()

	err = producer.Use("100k")
	handleError(err)

	for {
		job, err := consumer.ReserveWithTimeout(time.Second * 0)
		if err != nil {
			break;
		}

		err = consumer.Delete(job.Id)
		handleError(err)

		producer.Put(job.Body, 1024, time.Second * 0, time.Second * 600)
	}
	log.Println("Complete")
}

func handleError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
