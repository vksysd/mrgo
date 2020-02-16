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
	"time"
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
	jobDone                bool
	workerNum              int64
	ReducerFileState       map[string]int
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
	// fmt.Println("RequestTask getting called")
	// check what tyoe of task is ready ?
	m.c.L.Lock()
	task := MrReply{}

	isMapTaskAvailable := func() (string, true) {
		for _fileName, state := range m.filesState {
			if state == 0 {
				return _fileName, true
			}
		}
		return "", false
	}
	isAllMapDone := func() bool {
		for _, state := range m.filesState {
			if state != 2 {
				return false
			}
		}
		return true
	}
	isReduceTaskAvailable := func() (string, bool) {
		for _fileName, state := range m.ReducerFileState {
			if state == 0 {
				return _fileName, true
			}
		}
		return "", false
	}

	for {
		flname, flag := isMapTaskAvailable()
		if flag == true {
			task.FileName = flname
			task.WorkType = "Mapper"
			task.WorkerNum = m.workerNum + 1
			m.workerNum++
			m.filesState[task.FileName] = 1 // mapper in progress
			// start a timer
			// start a go routing and give the timer
			// that go routine should check the status of file given for the map task
			// if that status is 2 then mapper has done the job correctly
			// or else chnage the file state to 0 again
			timerx := time.NewTimer(time.Second * 10)
			go func(_flname string, _workerNum int) {
				<-timerx.C
				m.c.L.Lock()
				if m.filesState[_flname] != 2 {
					// mapper has not done its job in 10 sec
					m.filesState[_flname] = 0
					// if there are intermediate files related to the stopped mapper
					// remove those files also

					// this situation can be avoided if the mapper worker
					// creates temp files in a seperate directory
					// reply with those temp locations like ./xxDir/file.xyz
					// and master outs finalIntermediate files in different dir
				}

			}(task.FileName, task.WorkerNum)
			break
		} else {
			flag := isAllMapDone()
			if flag == true {
				if m.assembleFileDone == false {
					fileMap := make(map[string]*os.File)

					for i := 0; i < m.maxReducers; i++ {
						fName := "mapper" + "-" + strconv.Itoa(i)
						if _, err := os.Stat(fName); err == nil {
							fileMap[fName], err = os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
							if err != nil {
								log.Fatalln(err)
							}
						} else if os.IsNotExist(err) {
							fileMap[fName], err = os.Create(fName)
							if err != nil {
								log.Fatalln(err)
							}
						} else {
							fmt.Println("Something else is going on !")
						}
					}
					for _, fileName := range m.intermediateFiles {
						parts := strings.SplitN(fileName, "-", -1) // parts = [mapper,X,Y]
						fp, err := os.Open(fileName)               // open the file mapper-X-Y in read mode
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
					for idx, flname := range m.finalIntFiles {
						m.ReducerFileState[flname] = 0
					}
				}
				// All map tasks are done final Intermediate files are also ready
				flname, flag := isReduceTaskAvailable()
				if flag {
					task.FileName = flname
					task.WorkType = "Reducer"
					task.WorkerNum = m.workerNum + 1
					m.ReducerFileState[flname] = 1
					m.workerNum++
					break
				} else {
					// continue
					m.c.Wait() // sleep
					continue
				}
			} else {
				// or block
				m.c.Wait() // sleep
				continue
			}
		}

	}

	m.c.L.Unlock()
	*reply = rep
	return nil
}

func (m *Master) MapperDone(req *MapperRequest, reply *MrEmpty) error {

	for _, filename := range req.FileName {
		var fileExists bool
		for _, ele := range m.intermediateFiles {
			if ele == filename {
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
	m.c.L.Lock()
	m.filesState[req.OriginalFileAllocated] = req.MapperState
	m.c.L.Unlock()

	return nil
}

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
		for _, f := range m.intermediateFiles {
			var err = os.Remove(f)
			if err != nil {
				log.Fatalln(err)

			}
		}
		for _, f := range m.finalIntFiles {
			var err = os.Remove(f)
			if err != nil {
				log.Fatalln(err)
			}
		}
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
	m.jobDone = false
	m.workerNum = 0
	m.ReducerFileState = make(map[string]int)

	m.server()
	return &m
}
