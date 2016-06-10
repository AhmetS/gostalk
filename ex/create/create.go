package main

import (
	"github.com/AhmetS/gostalk"
	"log"
	"time"
	"encoding/json"
)

type payload struct {
	Id         int `json:"id"`
	Name       string `json:"name"`
	Created_at time.Time `json:"time"`
}

var g gostalk.Gostalk

func main() {

	g, err := gostalk.Connect("127.0.0.1:11300")
	if err != nil {
		log.Panicln(err)
	}
	defer g.Disconnect()
	g.Use("1m")

	for i := 0; i < 100000; i++ {
		payload, err := createNewPayload(i)
		if err != nil {
			log.Panicln(err)
		}
		jobId, err := g.Put(payload, 1024, time.Second * 0, time.Second * 600)
		if err != nil {
			log.Panicln(err)
		}
		log.Println("Put ", jobId)
	}
}

func createNewPayload(id int) (p []byte, err error) {

	myJob := new(payload)
	myJob.Id = id
	myJob.Created_at = time.Now()
	myJob.Name = "Test Message"

	p, err = json.Marshal(myJob)
	if err != nil {
		return nil, err
	}

	return p, err
}
