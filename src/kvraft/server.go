package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ddeka0/distributed-system/src/labgob"
	"github.com/ddeka0/distributed-system/src/labrpc"
	"github.com/ddeka0/distributed-system/src/raft"
)

// Debug ...
const Debug = 0

// DPrintf ...
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op ...
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string // "get" | "put" | "append"
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

// Result ...
type Result struct {
	Command   string
	OK        bool
	ClientID  int64
	RequestID int64
	NotLeader bool
	Err       Err
	Value     string
}

// KVServer ...
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db       map[string]string
	ack      map[int64]int64
	resultCh map[int]chan Result
}

func isMatch(entry Op, result Result) bool {
	return entry.ClientID == result.ClientID && entry.RequestID == result.RequestID
}
func (kv *KVServer) raftCall(entry Op) Result {
	index, _, isLeader := kv.rf.Start(entry) // this is the API call for raft
	if !isLeader {                           // check if this raft instance is not
		// the leader
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.resultCh[index]; !ok {
		kv.resultCh[index] = make(chan Result, 1) // prepare an asychronous point
		// for receving the raft result
	}
	kv.mu.Unlock()

	select { // wait for 240 millisecond before returning false
	case result := <-kv.resultCh[index]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(240 * time.Millisecond):
		return Result{OK: false}
	}
}

// Get ...
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "Get"
	entry.ClientID = args.ClientID
	entry.RequestID = args.RequestID
	entry.Key = args.Key
	// log.Println("New client request ", entry.Command, " by client ", entry.ClientID, " requestID = ", entry.RequestID)
	result := kv.raftCall(entry)
	if !result.OK {
		reply.NotLeader = true
		return
	}
	reply.NotLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
}

// PutAppend ...
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{}
	entry.Command = args.Op
	entry.ClientID = args.ClientID
	entry.RequestID = args.RequestID
	entry.Key = args.Key
	entry.Value = args.Value
	log.Println("New ", entry.Command, " request ", entry.Key, " : ", entry.Value)
	result := kv.raftCall(entry)
	if !result.OK {
		reply.NotLeader = true
		return
	}
	reply.NotLeader = false
	reply.Err = result.Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isDuplicated(op Op) bool {
	lastRequestID, ok := kv.ack[op.ClientID]
	if ok {
		return lastRequestID >= op.RequestID
	}
	return false
}

func (kv *KVServer) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.NotLeader = false
	result.ClientID = op.ClientID
	result.RequestID = op.RequestID

	switch op.Command {
	case "Put":
		if !kv.isDuplicated(op) { // check if op is new or not
			kv.db[op.Key] = op.Value // new value
			// log.Println("kv.db[op.Key] >>>>>>>>>>>>>>>>>>>>>>>>>", kv.db[op.Key])
		}
		result.Err = OK
	case "Append":
		if !kv.isDuplicated(op) {
			kv.db[op.Key] += op.Value // append
		}
		result.Err = OK
	case "Get":
		if value, ok := kv.db[op.Key]; ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	}
	kv.ack[op.ClientID] = op.RequestID // update the latest request ID
	// for this op.ClientID
	// log.Println("One operation applied ", op)
	return result
}

// Run ...
func (kv *KVServer) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		op := msg.Command.(Op)
		// log.Println(">>>>>>>>>>>> Raft accepted new OP", op)
		result := kv.applyOp(op)

		if ch, ok := kv.resultCh[msg.CommandIndex]; ok {
			select {
			case <-ch: // drain bad data
			default:
			}
		} else {
			kv.resultCh[msg.CommandIndex] = make(chan Result, 1)
		}
		kv.resultCh[msg.CommandIndex] <- result

		// create snapshot if raft state exceeds allowed size
		// if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		// 	w := new(bytes.Buffer)
		// 	e := labgob.NewEncoder(w)
		// 	e.Encode(kv.data)
		// 	e.Encode(kv.ack)
		// 	go kv.rf.CreateSnapshot(w.Bytes(), msg.CommandIndex)
		// }

		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Result)

	go kv.Run()
	return kv
}
