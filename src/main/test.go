package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const MAX_TIME = 3

var wg sync.WaitGroup

type notification struct {
	c chan chan string
}

func Schedular() {
	
}

func Request() {
	time.Sleep(time.Second * time.Duration(rand.Intn(MAX_TIME)))
	defer wg.Done()
	// inform the master go routine
	fmt.Println("Hello!")
}

func main() {
	wg.Add(1)
	go Request()

	wg.Add(1)
	go Request()
	wg.Wait()
}
