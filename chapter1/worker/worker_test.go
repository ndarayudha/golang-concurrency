package worker

import (
	"fmt"
	"math/rand"
	"testing"
)

type Work struct {
	value int
}

type Result struct {
	value int
}

func TestWorker(t *testing.T) {
	workCh := make(chan Work)
	resultCh := make(chan Result)
	done := make(chan bool)

	workQueue := make([]Work, 100)
	for i := range workQueue {
		workQueue[i].value = rand.Int()
	}
	// Create 10 worker
	for i := 0; i < 10; i++ {
		go func() {
			for {
				// Get work from the work channel
				work := <-workCh
				// Compute result
				result := Result{
					value: work.value * 2,
				}
				// Send the result via the result channel
				resultCh <- result
			}
		}()
	}
	results := make([]Result, 0)
	go func() {
		// Collect all the results
		for i := 0; i < len(workQueue); i++ {
			results = append(results, <-resultCh)
		}
		// When all the result are collected, notify the done channel
		done <- true
	}()
	// Send all the work to the workers
	for _, work := range workQueue {
		workCh <- work
	}
	// Wait until enverything is done
	<-done
	for _, res := range results {
		fmt.Println(res.value)
	}
}
