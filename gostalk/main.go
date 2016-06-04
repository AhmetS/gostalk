package main

import (
	"github.com/AhmetS/gostalk"
	"log"
)

func main() {

	g, err := gostalk.Connect("127.0.0.1:11300")
	if err != nil {
		log.Panicln(err)
	}
	defer g.Disconnect()


}
