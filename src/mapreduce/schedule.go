package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {

	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	c := make(chan string, 100)

	go func() {
		for {
			select {
			case srv, isOpen := <-registerChan:
				if isOpen {
					c <- srv
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(i int, c chan string, jobName string, mapFiles []string, phase jobPhase, n_other int) {
			success := false

			for !success {
				srv := <-c

				ok := call(srv, "Worker.DoTask", DoTaskArgs{
					JobName:       jobName,
					File:          mapFiles[i],
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: n_other,
				}, nil)
				if ok {
					wg.Done()
					success = true
				}
				c <- srv
			}


		}(i, c, jobName, mapFiles, phase, n_other)
	}
	wg.Wait()

	//close(registerChan)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
