package main

import (
	"github.com/AhmetS/gostalk"
	"log"
	"time"
	//"math/rand"
)

var tubeFrom string = "jobs1"
var tubeTo string = "jobs"
var numConsumers int = 2
var numProducers int = 2

func main() {

	// Example of normal channel
	jobCh := make(chan *gostalk.Job)

	// Example of buffered channel
	waitCh := make(chan bool, numConsumers)

	// Limit Max Consumers
	for j := 0; j < numConsumers; j++ {
		go func(j int) {

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
					log.Println("Closing Channel", j)
					waitCh <- true
					break
				}

				//log.Println("Receiving via Consumer:", j, string(job.Body))

				consumer.Delete(job.Id)

				jobCh <- job
			}
		}(j)
	}

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

	log.Println("Starting")
	n := 0
	for n < numConsumers {
		<-waitCh
		n++
	}

	close(jobCh)
	log.Println("Complete")
}

func handleError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
