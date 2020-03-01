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

// package main

// import (
// 	"log"
// 	"math/rand"
// 	"time"
// )

// const TimeOutTime = 3
// const MeanArrivalTime = 4

// func main() {
// 	const interval = time.Millisecond * TimeOutTime
// 	// channel for incoming messages
// 	var incomeCh = make(chan struct{})

// 	go func() {
// 		for {
// 			// On each iteration new timer is created
// 			t := time.NewTimer(interval)
// 			select {
// 			case <-t.C:
// 				//time.Sleep(time.Second)
// 				log.Println("Do task")
// 			case <-incomeCh:
// 				log.Println("Handle income message and move to the next iteration")
// 				t.Stop()
// 			}
// 		}
// 	}()

// 	go func() {
// 		for {
// 			time.Sleep(time.Duration(rand.Intn(MeanArrivalTime)) * time.Millisecond)
// 			// generate incoming message
// 			incomeCh <- struct{}{}
// 		}
// 	}()

// 	// prevent main to stop for a while
// 	<-time.After(100 * time.Second)
// }

package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const TimeOutTime = 5
const MeanArrivalTime = 3
const TimeUnit = time.Millisecond

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	var wg sync.WaitGroup
	wg.Add(1)
	// go routine for doing timeout event
	var msgC = make(chan struct{})
	go func() {
		defer wg.Done()
		d, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(3))
		for {
			t1 := time.Now()
			select {
			case <-d.Done():
				// Deadline expired.
				// ... handle it ...
				t2 := time.Now()
				fmt.Println("Election Started after timer expired after ", t2.Sub(t1))
				d, cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(3)) // if/as appropriate
			case <-msgC:
				// got message - cancel existing deadline and get new one
				t2 := time.Now()
				cancel()
				d, cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(3))
				fmt.Println("Election Timer Reset after ", t2.Sub(t1))
				// ... handle the message
			}
		}
	}()

	// go routine to simulate incoming messages ...
	go func() {
		for {
			// simulates a incoming message at any time
			time.Sleep(time.Second * time.Duration(MeanArrivalTime))
			//fmt.Println("messages arrived at ", v)

			msgC <- struct{}{}
		}
	}()

	wg.Wait()
}
