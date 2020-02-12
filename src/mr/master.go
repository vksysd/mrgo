package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	filesState             map[string]int
	files                  []string
	nMapper                int // total number of mapper worker
	nReducer               int
	intermediateFiles      []string
	reducerFiles           []string
	maxReducers            int
	completedReducersCount int
	mux                    sync.Mutex
	c                      *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// func Map(filename string, contents string) []mr.KeyValue
// func Reduce(key string, values []string) string

// func (m *Master) RequestTask(args)
// func Map(filename string, contents string) []mr.KeyValue
// func Reduce(key string, values []string) string
func (m *Master) RequestTask(req MrRequest, reply *MrReply) error {
	// dont use req
	fmt.Println("RequestTask getting called")
	rep := MrReply{}
	// 1. is all files are processed > ? map process is over ?
	// hold mutex lock
	m.c.L.Lock()
	if m.nMapper != 0 {
		//some filenames are not assigned to any Map Worker
		rep.FileName = m.files[m.nMapper-1]
		rep.WorkerNum = m.nMapper
		rep.WorkType = "Mapper"
		m.nMapper--
		m.filesState[rep.FileName] = 1
		fmt.Println(rep)

	} else {
		// all map tasks are done or not
		// check if all files are completed by Map workers
		allMapperDoneFlag := 1
		// lock it first

		for _, filename := range m.filesState {
			if filename != 2 {
				allMapperDoneFlag = 0
				break
			}
		}
		fmt.Println(m.filesState)
		if allMapperDoneFlag == 0 {
			// some tasks are still left;
			// make this RPC request on hold, using condition variables
			checkState := func() bool {
				for _, filename := range m.filesState {
					if filename != 2 {
						return false
					}
				}
				return true
			}
			for checkState() == false {
				m.c.Wait()
			}

		} else {
			//all mappers are fninished now we can start reducing
			//logic to start sending reducers work
			if m.nReducer != m.maxReducers {
				// some reducers task is still left, so assign it to a new worker
				rep.FileName = m.intermediateFiles[m.nReducer]
				rep.WorkerNum = m.nReducer + 1
				rep.WorkType = "Reducer"
				m.nReducer++
			} else {
				// all reduce tasks are finished
				// reply the worker to quit now, maybe close socket connection
				fmt.Printf("nothing here")
			}
		}
	}
	// release mutex
	// m.mux.Unlock()
	m.c.L.Unlock()
	// 2. if not send map task to worker
	*reply = rep
	// 3. otherwise send reduce task to worker
	return nil
}

func (m *Master) MapperDone(req *MapperRequest, reply *MrEmpty) error {

	for _, filename := range req.FileName {
		// check in intermediateFiles, if the filename do not exist append it
		var fileExists bool
		for _, ele := range m.intermediateFiles {
			if ele == filename {
				//file is already added
				fileExists = true
				break
			}
		}

		if !fileExists {
			m.c.L.Lock()
			m.intermediateFiles = append(m.intermediateFiles, filename)
			m.c.L.Unlock()
		}
	}
	// update the state of original file in the filesState map
	m.c.L.Lock()
	m.filesState[req.OriginalFileAllocated] = req.MapperState
	m.c.L.Unlock()

	return nil
}

// this rpc call is the status being sent from Reducer to Master once the reducer
// finishes its job
func (m *Master) ReducerDone(req *ReducerRequest, reply *MrEmpty) error {
	m.c.L.Lock()
	m.reducerFiles = append(m.reducerFiles, req.FileName)
	m.completedReducersCount += req.ReducerState
	m.c.L.Unlock()
	return nil
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("Example RPC is called")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done_() bool {
	ret := false

	// Your code here.
	// if all reducers have finished then say master is done
	m.c.L.Lock()
	if m.completedReducersCount == m.maxReducers {
		ret = true
	}
	m.c.L.Unlock()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.filesState = make(map[string]int)
	m.nMapper = 0
	m.intermediateFiles = make([]string, 0)
	m.reducerFiles = make([]string, 0)

	m.nReducer = 0
	m.maxReducers = nReduce
	m.completedReducersCount = 0
	for _, fileName := range files {
		m.filesState[fileName] = 0
		m.files = append(m.files, fileName)
		m.nMapper++
	}
	// Your code here.

	m.c = sync.NewCond(&m.mux)

	m.server()
	return &m
}
