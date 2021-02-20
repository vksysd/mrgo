package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	labrpc "github.com/ddeka0/distributed-system/src/labrpc"
)

// Clerk is ...basically works as a client
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID  int64
	RequestID int64
	Mtx       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk is ...creation of a client
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.RequestID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key              // set the key for this get request
	args.ClientID = ck.ClientID // set the client ID also
	ck.Mtx.Lock()               // take lock because this clerk object can be
	// used in different go routine in test
	args.RequestID = ck.RequestID
	ck.RequestID++
	ck.Mtx.Unlock()
	leader := 0 // start with first server
	for {       // infinite loop check later
		leader = (leader + 1) % len(ck.servers) // iterate over all the
		server := ck.servers[leader]            // take the server instance
		reply := GetReply{}
		ok := server.Call("KVServer.Get", &args, &reply)
		if ok && !reply.NotLeader {
			// log.Println("Get Value :", reply.Value)
			return reply.Value
		}
	}
}

// PutAppend ....
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key              // set the key for the Put or Append operation
	args.Value = value          // set the Value to be appended or Put
	args.Op = op                // Op can be "Put" or "Append"
	args.ClientID = ck.ClientID // set "which" client is asking this operation
	ck.Mtx.Lock()
	args.RequestID = ck.RequestID
	ck.RequestID++ //
	ck.Mtx.Unlock()
	// log.Println("client ... args >>>>>>>>>>>>>> ", args.Value == "")
	leader := 0 // start with first server
	for {       // infinite loop // toDO check later
		leader = (leader + 1) % len(ck.servers) // iterate over all the
		server := ck.servers[leader]            // take the server instance
		reply := PutAppendReply{}
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.NotLeader {
			return // we dont need to return anything from PurAppend function
		}
	}
}

// Put ...
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append ...
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
