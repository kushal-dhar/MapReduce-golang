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
		fmt.Println("phase: ",phase)
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		fmt.Println("phase: ",phase)
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	group := new(sync.WaitGroup)

	for i := 0; i < ntasks; i++ {
		var file string
		switch phase {
		case mapPhase:
			file = mapFiles[i]
		case reducePhase:
			file = ""
		}

		arg := &DoTaskArgs{
			JobName: jobName,
			File: file,
			Phase: phase,
			TaskNumber: i,
			NumOtherPhase: n_other,
		}
		group.Add(1)

		go func() {
			for worker := range registerChan {
				status := call(worker, "Worker.DoTask", arg, new(struct{}))
				fmt.Println(status)
				if status {
					go func() {
						registerChan <- worker
					} ()
					break
				}
			}
			group.Done()
		} ()
	}
	group.Wait()

	//fmt.Println("inside schedule: ",registerChan);
	/* if state == 0 {
		for i, file := range mapFiles {
			funcName := <- registerChan
			//fmt.Println("in for", funcName, file, i)
			funcName()
		}
	} */
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
