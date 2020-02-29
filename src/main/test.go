// package main

// import (
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"sync"
// 	"time"
// )

// const TimeOutTime = 5
// const MeanArrivalTime = 3

// func main() {
// 	rand.Seed(time.Now().UTC().UnixNano())
// 	var wg sync.WaitGroup
// 	t := time.NewTimer(time.Second * time.Duration(TimeOutTime))
// 	wg.Add(1)
// 	var mu sync.Mutex
// 	// go routine for doing timeout event
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			t1 := time.Now()
// 			fmt.Println("**waiting ...")
// 			<-t.C
// 			t2 := time.Now()
// 			// Do.. task X .. on timeout...
// 			log.Println("Timeout after ", t2.Sub(t1))
// 			mu.Lock()
// 			t.Reset(time.Second * time.Duration(TimeOutTime))
// 			mu.Unlock()
// 		}
// 	}()

// 	// go routine to simulate incoming messages ...
// 	go func() {
// 		for {
// 			// simulates a incoming message at any time
// 			time.Sleep(time.Second * time.Duration(MeanArrivalTime))
// 			//fmt.Println("messages arrived at ", v)

// 			// once any message is received reset the timer to 4 seconds again
// 			mu.Lock()
// 			t.Reset(time.Second * time.Duration(TimeOutTime))
// 			//t = time.NewTimer(time.Second * time.Duration(TimeOutTime))

// 			mu.Unlock()
// 		}
// 	}()

// 	wg.Wait()
// }

package main

import (
	"log"
	"math/rand"
	"time"
)

const TimeOutTime = 3
const MeanArrivalTime = 4

func main() {
	const interval = time.Millisecond * TimeOutTime
	// channel for incoming messages
	var incomeCh = make(chan struct{})

	go func() {
		for {
			// On each iteration new timer is created
			t := time.NewTimer(interval)
			select {
			case <-t.C:
				//time.Sleep(time.Second)
				log.Println("Do task")
			case <-incomeCh:
				log.Println("Handle income message and move to the next iteration")
				t.Stop()
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Duration(rand.Intn(MeanArrivalTime)) * time.Millisecond)
			// generate incoming message
			incomeCh <- struct{}{}
		}
	}()

	// prevent main to stop for a while
	<-time.After(100 * time.Second)
}
