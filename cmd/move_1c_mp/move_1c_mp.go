package main

import (
	"github.com/AhmetS/gostalk"
	"log"
	"time"
	//"math/rand"
)

func main() {

	jobCh := make(chan *gostalk.Job)

	// Max 5 Producer
	for i := 0; i < 5; i++ {

		go func(i int) {

			producer, err := gostalk.Connect("127.0.0.1:11300")
			handleError(err)

			err = producer.Use("100k1")
			handleError(err)

			for job := range jobCh {
				//time.Sleep(time.Second * time.Duration(rand.Intn(0)))
				log.Println("Sending via Producer:", i, string(job.Body))
				producer.Put(job.Body, 1024, time.Second * 0, time.Second * 600)
			}
		}(i)
	}

	consumer, err := gostalk.Connect("127.0.0.1:11300")
	handleError(err)
	defer consumer.Disconnect()

	_, err = consumer.Watch("100k")
	handleError(err)
	consumer.Ignore("default")
	handleError(err)

	for {
		//job, err := consumer.ReserveWithTimeout(time.Second * 0)
		job, err := consumer.Reserve()
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
