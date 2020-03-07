package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	labrpc "github.com/ddeka0/mrgo/src/labrpc"
)

const (
	// STATE_FOLLOWER .. Follwer state of raft server
	STATE_FOLLOWER = 0
	// STATE_CANDIDATE .. Follwer state of raft server
	STATE_CANDIDATE = 1
	// STATE_LEADER .. Follwer state of raft server
	STATE_LEADER = 2

	MAX_TIMEOUT = 500
	MIN_TIMEOUT = 150
)

// import "bytes"
// import "../labgob"

// ApplyMsg ...
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry  ...
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft ...
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// These are the states added from the Fig2 of raft paper
	CurrentTerm  int
	VotedFor     int
	commitIndex  int
	LastApplied  int
	NextIndex    []int
	MatchIndex   []int
	CurrentState int
	Mtx          sync.Mutex
	VoteCount    map[int]int
	msgC         chan struct{}
	Log          []LogEntry
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Mtx.Lock()
	term = rf.CurrentTerm
	isleader = (rf.CurrentState == STATE_LEADER)
	rf.Mtx.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// RequestVoteArgs ...
// Fill in the RequestVoteArgs and RequestVoteReply structs. Modify Make()
// to create a background goroutine that will kick off leader election periodically
// by sending out RequestVote RPCs when it hasn't heard from another peer for a
// while. This way a peer will learn who is the leader, if there is already a
// leader, or become the leader itself. Implement the RequestVote() RPC handler
// so that servers will vote for one another.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// Arguments:
	// term candidate’s term
	// candidateId candidate requesting vote
	// lastLogIndex index of candidate’s last log entry (§5.4)
	// lastLogTerm term of candidate’s last log entry (§5.4)
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// Results:
	// term currentTerm, for candidate to update itself
	// voteGranted true means candidate received vote
	Term        int
	VoteGranted bool
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *RequestVoteArgs, reply *RequestVoteReply) {

}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 	Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rep := RequestVoteReply{}
	rf.Mtx.Lock()
	var Voted bool
	if args.Term < rf.CurrentTerm {
		rep.VoteGranted = false
		rep.Term = rf.CurrentTerm
	} else {
		if rf.CurrentState == STATE_FOLLOWER {
			if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
				if len(rf.Log) == 0 {
					log.Println(rf.me, " voting for", args.CandidateID)
					Voted = true
					rf.VotedFor = args.CandidateID
				} else {
					if rf.Log[len(rf.Log)-1].Term <= args.LastLogTerm {
						Voted = true
					} else if rf.Log[len(rf.Log)-1].Term == args.LastLogTerm {
						if len(rf.Log) <= args.LastLogIndex {
							Voted = true
						}
					}
				}

			}
		}
	}
	rep.VoteGranted = Voted
	rep.Term = rf.CurrentTerm
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
	}
	*reply = rep
	rf.Mtx.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Println("got ", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.VoteCount = make(map[int]int)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Server when statrs up, starts a follower
	rf.CurrentState = STATE_FOLLOWER
	rf.msgC = make(chan struct{})
	rf.Log = make([]LogEntry, 0)
	//rf.Log = append(rf.Log, LogEntry{})
	rf.VotedFor = -1
	rand.Seed(time.Now().UTC().UnixNano())

	go func() {
		// kick off leader election periodically by sending out RequestVote RPCs
		// when it hasn't heard from another peer for a while

		d, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT+1)+MIN_TIMEOUT))
		// This is a timer checking go routine
		// this routine executes in a infinite for loop and checks if there
		// are timers events of not
		// if it gets a timer event:
		// -> spawns a new go routine R1, let R1 handle all the election related work
		t1 := time.Now()
		for {
			select {
			case <-d.Done():
				// Election Timer expired.
				t2 := time.Now()
				fmt.Println(me, " ]> Election Started after timer expired after ", t2.Sub(t1))
				d, cancel = context.WithTimeout(context.Background(), time.Millisecond*time.Duration(rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT+1)+MIN_TIMEOUT)) // if/as appropriate

				var electionOK bool

				rf.Mtx.Lock()
				//var _currentState = rf.CurrentState
				var _currentTerm = rf.CurrentTerm
				var _receivedTerm = _currentTerm // _receivedTerm is going to change
				var _candidateID = rf.me
				var _nServer = len(rf.peers)
				if rf.CurrentState == STATE_FOLLOWER || rf.CurrentState == STATE_CANDIDATE {
					electionOK = true
					rf.CurrentTerm++
					_currentTerm = rf.CurrentTerm
					rf.CurrentState = STATE_CANDIDATE
					//_currentState = STATE_CANDIDATE
					rf.VotedFor = -1 // TODO check
				}
				rf.Mtx.Unlock()

				if electionOK {
					go func() {

						var VoteCountForThisTerm int
						var localLock sync.Mutex
						var wg sync.WaitGroup

						localLock.Lock()
						VoteCountForThisTerm++ // Self Vote
						localLock.Unlock()
						for serverID := 0; serverID < _nServer; serverID++ {
							//log.Println("sent ", serverID)
							var req = RequestVoteArgs{}
							var rep = RequestVoteReply{}
							req.CandidateID = _candidateID
							req.LastLogIndex = 0
							req.LastLogTerm = 0
							req.Term = _currentTerm
							if serverID != _candidateID {
								// Create a New Go Routine to Send And Block for Receive
								wg.Add(1)
								go func(_serverID int) {
									defer wg.Done()
									ok := rf.sendRequestVote(_serverID, &req, &rep)
									//log.Println("RPC responded!")
									//log.Println(rep)
									if !ok {
										// log.Println("Error in Sending RequestVote RPC!")
									} else {
										if rep.Term > _currentTerm {
											// rf.Mtx.Lock()
											// // TODO move to followe state
											// // stop processing this election votes or
											// // sending requestVote rpc
											// // TODO check
											// rf.CurrentState = STATE_FOLLOWER
											// rf.Mtx.Unlock()
											localLock.Lock()
											_receivedTerm = max(_receivedTerm, rep.Term)
											localLock.Unlock()
										} else {
											if rep.VoteGranted {
												localLock.Lock()
												log.Println("[", _candidateID, "]>", " got 1 vote for term ", _currentTerm)
												VoteCountForThisTerm++
												localLock.Unlock()
											}
										}
									}
								}(serverID)
							}
						}
						wg.Wait()

						rf.Mtx.Lock()
						log.Println("[", rf.me, "]> my current term", rf.CurrentTerm)
						if _receivedTerm > _currentTerm {
							rf.CurrentState = STATE_FOLLOWER
							rf.CurrentTerm = _receivedTerm
						} else {
							if rf.CurrentState == STATE_CANDIDATE && rf.CurrentTerm == _currentTerm {
								log.Println("[", rf.me, "]>", "VoteCountForThisTerm (", rf.CurrentTerm, ") = ", VoteCountForThisTerm)
								if VoteCountForThisTerm > (len(rf.peers) / 2) {
									rf.CurrentState = STATE_LEADER
									log.Println(me, " : I am a leader now !!!!!!!!!!!!!!!!!!!!!!!!")
									// Send messages to AppendEntried go routine to send
									// heartbeats to the followers
									// that go routine will sleep for some time and wakes up
									// check this state
									// if LEADER then sends out HeartBeats
								}
							} else {
								//log.Println("Something might have happened in between!")
							}
						}
						rf.Mtx.Unlock()
					}()
				}
			case <-rf.msgC:
				// got message - cancel existing ElectionTimer and get new one
				// t2 := time.Now()
				cancel()
				d, cancel = context.WithTimeout(context.Background(), time.Millisecond*time.Duration(rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT+500)+MIN_TIMEOUT))
				// fmt.Println("Election Timer Reset after ", t2.Sub(t1))
			}
		}
	}()

	return rf
}
