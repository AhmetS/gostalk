package main

import (
	"github.com/AhmetS/gostalk"
	"log"
	"time"
	//"math/rand"
)

var tubeFrom string = "100k1"
var tubeTo string = "100k"
var numConsumers int = 1
var numProducers int = 1

func main() {

	jobCh := make(chan *gostalk.Job)

	// Limit Producers
	for i := 0; i < numProducers; i++ {

		go func(i int) {

			producer, err := gostalk.Connect("127.0.0.1:11300")
			handleError(err)

			err = producer.Use(tubeTo)
			handleError(err)

			for job := range jobCh {
				//time.Sleep(time.Second * time.Duration(rand.Intn(0)))
				//log.Println("Sending via Producer:", i, string(job.Body))
				producer.Put(job.Body, 1024, time.Second * 0, time.Second * 600)
			}
		}(i)
	}

	consumer, err := gostalk.Connect("127.0.0.1:11300")
	handleError(err)
	defer consumer.Disconnect()

	_, err = consumer.Watch(tubeFrom)
	handleError(err)
	consumer.Ignore("default")
	handleError(err)

	for {
		job, err := consumer.ReserveWithTimeout(time.Second * 0)
		//job, err := consumer.Reserve()
		if err != nil {
			log.Println("Closing Channel")
			close(jobCh)
			break
		}

		consumer.Delete(job.Id)

		jobCh <- job
	}

	log.Println("Complete")
}

func handleError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
