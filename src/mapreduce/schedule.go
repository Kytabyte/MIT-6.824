package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
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

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// The scheduler schedules tasks as follows (no fault tolerance):
	// for each task:
	// 1. wait for available workers appear in `registerChan`
	// 2. send worker tasks through new routines
	// 3. send worker to `registerChan` after job is finished
	// 4. use a sync.WaitGroup to wait for all tasks done

	// For handling worker's failure
	// 1. check result for the RPC call, keep running until succeed
	// 2. if a worker fails, do not send it back to `registerChan`

	var wg sync.WaitGroup
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		// `i` is changing in this function, we need to assign relative var into other
		// vars before starting the goroutines
		var mapFile string
		if phase == mapPhase {
			mapFile = mapFiles[i]
		}
		taskNumber := i

		go func() {
			ok := false
			var worker string
			for !ok {
				worker = <-registerChan // blocked until there is a available worker
				taskArgs := DoTaskArgs{
					JobName:       jobName,
					File:          mapFile,
					Phase:         phase,
					TaskNumber:    taskNumber,
					NumOtherPhase: n_other}
				ok = call(worker, "Worker.DoTask", taskArgs, new(struct{}))
			}
			go func() { registerChan <- worker }() // otherwise this instruction may be blocked
			wg.Done()
		}()
	}

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
