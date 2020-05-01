package mr

// ExampleArgs is ...
type ExampleArgs struct {
	X int
}

// ExampleReply is ...
type ExampleReply struct {
	Y int
}

// MrReply is ...
type MrReply struct {
	FileName  string
	WorkerNum int
	WorkType  string
}

// MapperRequest is...
type MapperRequest struct {
	FileName              []string
	OriginalFileAllocated string
	WorkerNum             int
}

// ReducerRequest is ...
type ReducerRequest struct {
	FileName              string
	OriginalFileAllocated string
	WorkerNum             int
}

// MrRequest is ...
type MrRequest struct {
}

// MrEmpty is ...
type MrEmpty struct {
}

// Add your RPC definitions here.
