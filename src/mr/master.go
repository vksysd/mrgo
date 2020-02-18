package mr

import (
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
	rep := MrReply{}

	m.c.L.Lock()
	if m.nMapper != 0 {
		rep.FileName = m.files[m.nMapper-1]
		rep.WorkerNum = m.nMapper
		rep.WorkType = "Mapper"
		m.nMapper--
		m.filesState[rep.FileName] = 1
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
				// dirName := filepath.Dir(filePath)          // = 7
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
		}

		if m.nReducer != m.maxReducers {
			rep.FileName = m.finalIntFiles[m.nReducer]
			rep.WorkerNum = m.nReducer + 1
			rep.WorkType = "Reducer"
			m.nReducer++
			// fmt.Println("File Sent to Reducer = ", rep.FileName)
		} else {
			fmt.Println("All Tasks are assigned")
			rep.FileName = "None"
			rep.WorkerNum = -1
			rep.WorkType = "None"
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

	m.server()
	return &m
}
