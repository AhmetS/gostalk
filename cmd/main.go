package main

import (
	"github.com/AhmetS/gostalk"
	"log"
	"time"
	//"encoding/json"
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

	_, err = g.Watch("aaaaa")
	if err != nil {
		log.Panicln(err)
	}

	_, err = g.Ignore("default")
	if err != nil {
		log.Panicln(err)
	}

	for {
		job, err := g.ReserveWithTimeout(time.Second * 0)
		if err != nil {
			if err.Error() == "TIMED_OUT" {
				log.Println("Timed Outf")
				continue
			}
			log.Panicln(err)
		}

		log.Println("Got Job: ", job.Id, string(job.Body))

		time.Sleep(time.Second * 0)

		err = g.Delete(job.Id)
		if err != nil {
			log.Panicln(err)
		}
		log.Println("Job Deleted")
	}

	//err = g.Use("test_100k_conc");
	//if err != nil {
	//	log.Panicln(err.Error())
	//}

	//g.Watch("test_1m")
	//g.Ignore("default")
	//
	//for i := 0; i < 5; i++ {
	//
	//	p, _ := createNewPayload(i)
	//
	//	g.Put(p, 1024, 0, 60)
	//}

	//g.Watch("po_refresh_match")
	//g.Ignore("default")
	//
	//for i := 0; i < 39617; i++ {
	//	job, err := g.Reserve()
	//	if err != nil {
	//		log.Panicln(err.Error())
	//	}
	//	g.Delete(job.Id)
	//}
}
//
//func createNewPayload(id int) (p []byte, err error) {
//
//	myJob := new(payload)
//	myJob.Id = id
//	myJob.Created_at = time.Now()
//	myJob.Name = "Test Message"
//
//	p, err = json.Marshal(myJob)
//	if err != nil {
//		return nil, err
//	}
//
//	return p, err
//}
