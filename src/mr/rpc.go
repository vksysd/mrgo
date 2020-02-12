package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MrReply struct {
	FileName  string
	WorkerNum int
	WorkType  string
}

type MapperRequest struct {
	FileName              []string
	MapperState           int
	OriginalFileAllocated string
}

type ReducerRequest struct {
	FileName string
	// once the reducer completes it sends its state as 1
	ReducerState int
}

type MrRequest struct {
}

type MrEmpty struct {
}

// Add your RPC definitions here.
