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
	workerNum              int
	ReducerFileState       map[string]int
	InvalidMapperWorker    map[int]int
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
	isAllReduceDone := func() bool {
		for _, state := range m.ReducerFileState {
			if state != 2 {
				return false
			}
		}
		return true
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
			ctx, _ := context.WithTimeout(context.Background(), time.Duration(4)*time.Second)
			// start a go routing and give the timer
			go func(n int, flName string) {
				<-ctx.Done()
				m.c.L.Lock()

				if m.filesState[flName] != 2 {
					// it means the mapper worker has not completed the work in time
					// therefore make the filestate of that file to unprocessed or ready to process
					// which is 1
					m.filesState[flName] = 0
					m.InvalidMapperWorker[n] = 1 // do not process MapperDone() RPC
					fmt.Println("Timer expired for Mapper Worker ", n, " filename ", flName)
					fmt.Println("Work will be reassigned to other mapper")
				}
				// check if
				m.c.L.Unlock()
			}(task.WorkerNum, flname)
			// that go routine should check the status of file given for the map task
			// if that status is 2 then mapper has done the job correctly
			// or else chnage the file state to 0 again
			// TODO TIMER IMPLEMENT
			// timerx := time.NewTimer(time.Second * 10)
			// go func(_flname string, _workerNum int) {
			// 	<-timerx.C
			// 	m.c.L.Lock()
			// 	if m.filesState[_flname] != 2 {
			// 		// mapper has not done its job in 10 sec
			// 		m.filesState[_flname] = 0
			// 		// if there are intermediate files related to the stopped mapper
			// 		// remove those files also

			// 		// this situation can be avoided if the mapper worker
			// 		// creates temp files in a seperate directory
			// 		// reply with those temp locations like ./xxDir/file.xyz
			// 		// and master outs finalIntermediate files in different dir
			// 	}

			// }(task.FileName, task.WorkerNum)
			fmt.Println("file ", flname, " is given to worker no ", task.WorkerNum)
			break
		} else {
			flag := isAllMapDone()
			if flag == true {
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
							fmt.Println("Something else is going on !")
						}
					}
					for _, filePath := range m.intermediateFiles {
						// filePath = /7/mapper-7-4
						fileName := filepath.Base(filePath) // = mapper-7-4
						_dirName := filepath.Dir(filePath)
						wkn, err := strconv.Atoi(_dirName)
						if err != nil {
							log.Println(err)
						}
						// if this directory belongs to a invalidated worker
						// do not process the directory and ignore
						if _, ok := m.InvalidMapperWorker[wkn]; ok {
							//fmt.Println("Work of mapper worker ", wkn, " is ignored!")
							continue
						}
						// = 7
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
					for _, flname := range m.finalIntFiles {
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
					fmt.Println("file ", flname, " is given to worker no ", task.WorkerNum)
					break
				} else {
					// continue
					flag := isAllReduceDone()
					if flag {
						fmt.Println("All Tasks are assigned")
						task.FileName = "None"
						task.WorkerNum = -1
						task.WorkType = "None"
						break
					} else {
						// some reduce work are going on .. wait ...
						m.c.Wait() // sleep
						continue
					}

				}
			} else {
				// or block
				m.c.Wait() // sleep
				continue
			}
		}

	}

	m.c.L.Unlock()
	*reply = task
	return nil
}

func (m *Master) MapperDone(req *MapperRequest, reply *MrEmpty) error {
	//fmt.Println(req)
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
			// change the state only if the worker is is a valid one
			m.intermediateFiles = append(m.intermediateFiles, filename)
			m.c.L.Unlock()
		}
	}

	m.c.L.Lock()
	_, ok := m.InvalidMapperWorker[req.WorkerNum]
	if ok == false {
		m.filesState[req.OriginalFileAllocated] = 2
	} else {
		fmt.Println("Intermediate files from worker = ", req.WorkerNum, " has been rejected for original file", req.OriginalFileAllocated)
	}
	m.c.L.Unlock()

	m.c.Signal()

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
		m.c.Broadcast() // tell all other reducer to mapper RPC handlers to
		// wake up
		time.Sleep(time.Second)
		ret = true
		dirsSet := make(map[string]bool)
		for _, f := range m.intermediateFiles {
			if _, ok := dirsSet[filepath.Dir(f)]; !ok {
				// filepath.Dir(f) does not exisits in the candidate delete dirs
				dirsSet[filepath.Dir(f)] = true
			}
		}
		for _dir, _ := range dirsSet {
			fmt.Println("Removing dir ...", _dir)
			// os.Remove(emptydir)
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
	m.InvalidMapperWorker = make(map[int]int)

	m.server()
	return &m
}
