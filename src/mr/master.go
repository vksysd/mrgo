package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	finalIntFiles          []string
	maxReducers            int
	completedReducersCount int
	mux                    sync.Mutex
	c                      *sync.Cond
	assembleFileDone       bool
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

		checkState := func() bool {
			for _, state := range m.filesState {
				if state != 2 {
					return false
				}
			}
			return true
		}
		for checkState() == false {
			m.c.Wait()
		}

		if m.assembleFileDone == false {
			fileMap := make(map[string]*os.File)

			for i := 0; i < m.maxReducers; i++ {
				fName := "mapper" + "-" + strconv.Itoa(i)
				if _, err := os.Stat(fName); err == nil {
					fileMap[fName], err = os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
					if err != nil {
						log.Fatalln(err)
					}
					//defer fileMap[fName].Close()
				} else if os.IsNotExist(err) {
					fileMap[fName], err = os.Create(fName)
					if err != nil {
						log.Fatalln(err)
					}
					//defer fileMap[fName].Close()
				} else {
					fmt.Println("Something else is going on !")
				}
			}
			for _, fileName := range m.intermediateFiles {
				parts := strings.SplitN(fileName, "-", -1) // parts = [mapper,X,Y]
				//fmt.Println(parts[2])
				fp, err := os.Open(fileName) // open the file mapper-X-Y in read mode
				if err != nil {
					log.Fatalln(err)
				}
				_, err = io.Copy(fileMap["mapper"+"-"+parts[2]], fp)
				if err != nil {
					log.Fatalln(err)
				}
				fp.Close()
			}
			for fl, _ := range fileMap {
				m.finalIntFiles = append(m.finalIntFiles, fl)
			}
			for _, v := range fileMap {
				v.Close()
			}
			m.assembleFileDone = true
		}

		if m.nReducer != m.maxReducers {
			// some reducers task is still left, so assign it to a new worker
			rep.FileName = m.finalIntFiles[m.nReducer]
			rep.WorkerNum = m.nReducer + 1
			rep.WorkType = "Reducer"
			m.nReducer++
			fmt.Println("File Sent to Reducer = ", rep.FileName)
		} else {
			// all reduce tasks are finished
			// reply the worker to quit now, maybe close socket connection
			fmt.Printf("nothing here")
		}

	}
	m.c.L.Unlock()
	*reply = rep
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
	m.c.Broadcast() // special use
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
	m.finalIntFiles = make([]string, 0)
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
	m.assembleFileDone = false

	m.server()
	return &m
}
