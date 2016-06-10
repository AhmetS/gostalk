package main

import (
	"fmt"
	"time"
)

func main() {

	ch := make(chan int, 3)
	ch <- 1
	fmt.Println("Sleeping for 1 second")
	time.Sleep(time.Second)
	ch <- 2
	fmt.Println("Sleeping for 1 second")
	time.Sleep(time.Second)
	ch <- 3

// Overfilled Buffer
//	fmt.Println("Sleeping for 1 second")
//	time.Sleep(time.Second)
//	ch <- 9

	fmt.Println(<-ch)
	fmt.Println(<-ch)
	fmt.Println(<-ch)
}
