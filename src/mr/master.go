package mr

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAPPER_NOT_STARTED  = 0
	MAPPER_IN_PROGRESS  = 1
	MAPPER_DONE         = 2
	REDUCER_NOT_STARTED = 3
	REDUCER_IN_PROGRESS = 4
	REDUCER_DONE        = 5
	MAPPER_WORK         = "Mapper"
	REDUCE_WORK         = "Reducer"
	TIME_OUT_MAPPER     = 10
	TIME_OUT_REDUCER    = 10
)

type Master struct {
	// Your definitions here.
	filesState             map[string]int
	files                  []string
	nMapper                int // total number of mapper worker
	nReducer               int
	intermediateFiles      []string
	finalIntFiles          []string
	maxReducers            int
	completedReducersCount int
	mux                    sync.Mutex
	c                      *sync.Cond
	assembleFileDone       bool
	jobDone                bool
	workerNum              int
	ReducerFileState       map[string]int
	InvalidMapperWorker    map[int]int
	InvalidReduceWorker    map[int]int
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

	isMapTaskAvailable := func() (string, bool) {
		for _fileName, state := range m.filesState {
			if state == MAPPER_NOT_STARTED {
				return _fileName, true
			}
		}
		return "", false
	}
	isAllMapDone := func() bool {
		for _, state := range m.filesState {
			if state != MAPPER_DONE {
				return false
			}
		}
		return true
	}
	isReduceTaskAvailable := func() (string, bool) {
		for _fileName, state := range m.ReducerFileState {
			if state == REDUCER_NOT_STARTED {
				return _fileName, true
			}
		}
		return "", false
	}
	isAllReduceDone := func() bool {
		for _, state := range m.ReducerFileState {
			if state != REDUCER_DONE {
				return false
			}
		}
		return true
	}
	for {
		flname, flag := isMapTaskAvailable()
		if flag == true {
			fmt.Println("Map task available")
			task.FileName = flname
			task.WorkType = MAPPER_WORK
			task.WorkerNum = m.workerNum + 1
			m.workerNum++
			m.filesState[task.FileName] = MAPPER_IN_PROGRESS // mapper in progress

			// start a timer
			ctx, _ := context.WithTimeout(context.Background(), time.Duration(
				TIME_OUT_MAPPER)*time.Second)
			// start a go routing to handle timeout
			go func(n int, flName string) {
				<-ctx.Done()
				m.c.L.Lock()

				if m.filesState[flName] != MAPPER_DONE {
					// it means the mapper worker has not completed the work in
					// time therefore => make the filestate of that file to
					// MAPPER_NOT_STARTED
					m.filesState[flName] = MAPPER_NOT_STARTED
					m.InvalidMapperWorker[n] = 1 // do not process MapperDone() RPC
					fmt.Println("Timeout for Mapper Worker ", n, " filename ", flName)
				}
				// also wake up one or more sleeping worker for map task
				m.c.L.Unlock()
				m.c.Signal()
			}(task.WorkerNum, task.FileName)

			fmt.Println("File ", flname, " is given to worker no ", task.WorkerNum)
			// done with assigning a mapper task, break the loop
			break
		} else {
			flag := isAllMapDone()
			if flag == true {
				fmt.Println("ALl mapper done!")
				if m.assembleFileDone == false {
					dirName := "mr-intermediate"
					err := os.Mkdir(dirName, 0755)
					if err != nil {
						log.Fatalln(err)
					}
					//files are : /mr-intermediate/mapper-Y
					fileMap := make(map[string]*os.File)

					for i := 0; i < m.maxReducers; i++ {
						fName := "mapper" + "-" + strconv.Itoa(i)
						fName = filepath.Join(dirName, fName)
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
							fmt.Println("Unexpected Error occured in Creating File")
						}
					}
					for _, filePath := range m.intermediateFiles {
						// filePath can be of type /X/mapper-X-Y
						fileName := filepath.Base(filePath) // = mapper-7-4
						_dirName := filepath.Dir(filePath)
						// get the worknum corresponding to this file
						wkn, err := strconv.Atoi(_dirName)
						if err != nil {
							log.Println(err)
						}
						// if this directory belongs to a invalidated worker
						// do not process the directory and ignore
						if _, ok := m.InvalidMapperWorker[wkn]; ok {
							continue
						}
						parts := strings.SplitN(fileName, "-", -1) // parts = [mapper,X,Y]
						fp, err := os.Open(filePath)               // open the file /7/mapper-7-4 in read mode
						if err != nil {
							log.Fatalln(err)
						}
						_, err = io.Copy(fileMap[filepath.Join(dirName, "mapper"+"-"+parts[2])], fp)
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
					// initialize the states of final intermediate files
					// set them to REDUCER_NOT_STARTED
					for _, flname := range m.finalIntFiles {
						m.ReducerFileState[flname] = REDUCER_NOT_STARTED
					}
				}
				// All map tasks are done final Intermediate files are also ready
				flname, flag := isReduceTaskAvailable()
				if flag {
					fmt.Println("Reduce task available")
					task.FileName = flname
					task.WorkType = REDUCE_WORK
					task.WorkerNum = m.workerNum + 1
					m.ReducerFileState[flname] = REDUCER_IN_PROGRESS
					m.workerNum++
					ctx, _ := context.WithTimeout(context.Background(), time.Duration(
						TIME_OUT_REDUCER)*time.Second)
					// start a go routing to handle timeout
					go func(n int, flName string) {
						<-ctx.Done()

						m.c.L.Lock()

						if m.ReducerFileState[flName] != REDUCER_DONE {
							// it means the reducer worker has not completed the work in
							// time therefore => make the filestate of that file to
							// REDUCER_NOT_STARTED
							m.ReducerFileState[flName] = REDUCER_NOT_STARTED
							m.InvalidReduceWorker[n] = 1 // do not process ReduceDone() RPC
							fmt.Println("Timeout Reducer Worker ", n, " filename ", flName)
						}
						m.c.L.Unlock()

						m.c.Signal()
					}(task.WorkerNum, task.FileName)
					fmt.Println("file ", flname, " is given to worker no ", task.WorkerNum)
					break
				} else {
					// continue
					flag := isAllReduceDone()
					if flag {
						fmt.Println("All Reduce Tasks are assigned")
						task.FileName = "None"
						task.WorkerNum = -1
						task.WorkType = "None"
						break

					} else {
						// some reduce work are going on .. wait ...
						fmt.Println("Sleep in the wait of reduce task")
						m.c.Wait() // sleep
						fmt.Println("Woke up after sleep for reduce work")
						continue
					}

				}
			} else {
				// or block
				fmt.Println("Sleep in the wait of map task")
				m.c.Wait() // sleep
				fmt.Println("Woke up after sleep for map work")
				continue
			}
		}

	}

	m.c.L.Unlock()
	*reply = task
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
	_, ok := m.InvalidMapperWorker[req.WorkerNum]
	if ok == false {
		m.filesState[req.OriginalFileAllocated] = MAPPER_DONE
	} else {
		fmt.Println("Intermediate files from worker = ", req.WorkerNum, " has been rejected for original file", req.OriginalFileAllocated)
	}
	m.c.L.Unlock()
	m.c.Signal() // to wake up any worker RPC handler thread

	return nil
}

func (m *Master) ReducerDone(req *ReducerRequest, reply *MrEmpty) error {
	m.c.L.Lock()
	_, ok := m.InvalidReduceWorker[req.WorkerNum]
	if ok == false {
		m.ReducerFileState[req.OriginalFileAllocated] = REDUCER_DONE
		m.completedReducersCount += 1
	} else {
		fmt.Println("Output file from worker = ", req.WorkerNum, " has been rejected for original file", req.OriginalFileAllocated)
		// immediately delete the output file procedured by this invalidated
		// reduce worker
		err := os.Remove(req.FileName)
		if err != nil {
			log.Println(err)
		}
	}

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
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// if all reducers have finished then say master is done
	m.c.L.Lock()
	if m.completedReducersCount == m.maxReducers {
		m.c.Broadcast() // tell all other reducer to mapper RPC handlers to
		// wake up
		time.Sleep(time.Second)
		ret = true
		dirsSet := make(map[string]bool)
		for _, f := range m.intermediateFiles {
			if _, ok := dirsSet[filepath.Dir(f)]; !ok {
				dirsSet[filepath.Dir(f)] = true
			}
		}
		for _dir, _ := range dirsSet {
			err := os.RemoveAll(_dir)
			if err != nil {
				log.Println(err)
			}
		}
		err := os.RemoveAll("mr-intermediate")
		if err != nil {
			log.Println(err)
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
	m.InvalidMapperWorker = make(map[int]int)
	m.InvalidReduceWorker = make(map[int]int)

	m.server()
	return &m
}
