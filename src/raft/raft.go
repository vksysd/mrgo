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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ddeka0/distributed-system/src/labgob"
	labrpc "github.com/ddeka0/distributed-system/src/labrpc"
	log "github.com/sirupsen/logrus"
)

const (
	// STATE_FOLLOWER .. Follwer state of raft server
	STATE_FOLLOWER = 0
	// STATE_CANDIDATE .. Follwer state of raft server
	STATE_CANDIDATE = 1
	// STATE_LEADER .. Follwer state of raft server
	STATE_LEADER = 2

	MAX_TIMEOUT = 500
	MIN_TIMEOUT = 200
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
	Index   int
	Term    int
	Command interface{}
}

// Raft ...
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// These are the states added from the Fig2 of raft paper
	CurrentTerm      int
	VotedFor         int
	commitIndex      int
	LastApplied      int
	NextIndex        []int
	MatchIndex       []int
	CurrentState     int
	Mtx              sync.Mutex
	VoteCount        map[int]int
	Log              []LogEntry
	nServer          int
	applyCh          chan ApplyMsg
	GrantVoteChannel chan bool
	WinElectChannel  chan bool
	HeartbeatChannel chan bool
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
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}

//
// encode current raft state.
//
func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	return w.Bytes()
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

// AppendEntry is ...
// Arguments:
// term leader’s term
// leaderId so follower can redirect clients
// prevLogIndex index of log entry immediately preceding
// new ones
// prevLogTerm term of prevLogIndex entry
// entries[] log entries to store (empty for heartbeat;
// may send more than one for efficiency)
// leaderCommit leader’s commitIndex
// Results:
// term currentTerm, for leader to update itself
// success true if follower contained entry matching
// prevLogIndex and prevLogTerm
type AppendEntryArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

// AppendEntryReply is ...
type AppendEntryReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

func (rf *Raft) getLastLogTerm() int {
	return rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.Log[len(rf.Log)-1].Index
}

func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

//
// apply log entries with index in range [lastApplied + 1, commitIndex]
//
func (rf *Raft) applyLog() {
	rf.Mtx.Lock()
	defer rf.Mtx.Unlock()

	baseIndex := rf.Log[0].Index

	for i := rf.LastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.Log[i-baseIndex].Command
		rf.applyCh <- msg
	}
	rf.LastApplied = rf.commitIndex
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.Mtx.Lock()
	defer rf.Mtx.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.CurrentTerm {
		// reject requests with stale term number
		reply.Term = rf.CurrentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.CurrentTerm {
		// become follower and update current term
		rf.CurrentState = STATE_FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	// confirm heartbeat to restart timer
	rf.HeartbeatChannel <- true

	reply.Term = rf.CurrentTerm

	// if my log is very small than the leader's
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	baseIndex := rf.Log[0].Index
	// this if means Entries is non zero and we have to check conditions
	// check prevIndex entry: try to match the term with args.PrevLogTerm
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.Log[args.PrevLogIndex-baseIndex].Term {
		log.WithFields(log.Fields{
			"server ": rf.me,
			"term ":   rf.CurrentTerm,
		}).Warn("==Mismatch of logs")
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict entries before.
		// bypass all entries during the problematic term to speed up.
		// Here we are skipping one bad term...
		term := rf.Log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if rf.Log[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex {
		// time to correct or copy new entries
		// first copy completely uptp prevIndex + 1 is for inclusing prevIndex
		rf.Log = rf.Log[:args.PrevLogIndex-baseIndex+1]
		// beyond prevIndex overwrite new entries
		rf.Log = append(rf.Log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries) // not used

		if rf.commitIndex < args.LeaderCommitIndex {
			// here this server (a followe )
			rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	//log.Println("got ", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.Mtx.Lock()
	defer rf.Mtx.Unlock()
	defer rf.persist()

	if args.Term < rf.CurrentTerm {
		// reject request with stale term number
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentState {
		// become follower and update current term
		rf.CurrentState = STATE_FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	// rf.VotedFor == args.CandidateID this case is added to consider the drop
	// of messages when this server has already voted for args.CandidateID
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// vote for the candidate
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		log.WithFields(log.Fields{
			"by server ": rf.me,
			"@ term":     rf.CurrentTerm,
		}).Debug("voted for server ", "[", args.CandidateID, "]")
		rf.GrantVoteChannel <- true
	} else {
		// this else section is just for logging purpose
		k := rf.isUpToDate(args.LastLogTerm, args.LastLogIndex)
		if !k {
			log.WithFields(log.Fields{
				"by server ": rf.me,
				"@ term ":    rf.CurrentTerm,
			}).Error("Candidate", "[", args.CandidateID, "]", " log is not up to date")
		}
	}
}

// log.Trace("Something very low level.")
// log.Debug("Useful debugging information.")
// log.Info("Something noteworthy happened!")
// log.Warn("You should probably take a look at this.")
// log.Error("Something failed but I'm not quitting.")
// // Calls os.Exit(1) after logging
// log.Fatal("Bye.")
// // Calls panic() after logging
// log.Panic("I'm bailing.")
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
	rf.Mtx.Lock()
	defer rf.Mtx.Unlock()
	isLeader := false
	_commitIndex := 0 // where this command is going to be commited if it ever does
	_term := 0
	if rf.CurrentState == STATE_LEADER {
		isLeader = true
	}
	if isLeader {
		_term = rf.CurrentTerm
		_commitIndex = rf.getLastLogIndex() + 1
		rf.Log = append(rf.Log, LogEntry{Index: _commitIndex, Term: _term, Command: command})
		rf.persist()
	}
	// rf.HeartbeatChannel <- true
	return _commitIndex, _term, isLeader
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
func (rf *Raft) _sendAppendEntries() {
	rf.Mtx.Lock()
	defer rf.Mtx.Unlock()
	// this function will return immediately
	// it starts one go routine for each follower to send new entries
	// and those go routine waits for answer
	for serverID := 0; serverID < rf.nServer; serverID++ {
		if serverID != rf.me && rf.CurrentState == STATE_LEADER {
			req := AppendEntryArgs{}
			rep := AppendEntryReply{}
			baseIndex := rf.Log[0].Index
			req.Term = rf.CurrentTerm
			req.LeaderID = rf.me
			req.PrevLogIndex = rf.NextIndex[serverID] - 1
			if req.PrevLogIndex >= baseIndex {
				if len(rf.Log) > (req.PrevLogIndex - baseIndex) {
					req.PrevLogTerm = rf.Log[req.PrevLogIndex-baseIndex].Term
				} else {
					log.WithFields(log.Fields{
						"PreviousLogIndex ": req.PrevLogIndex,
						"baseindex ":        baseIndex,
					}).Error("Index out of bound error !! Returning from _sendAppendEntries")
					return
				}
			}

			if rf.NextIndex[serverID] <= rf.getLastLogIndex() {
				req.Entries = rf.Log[rf.NextIndex[serverID]-baseIndex:]
			}
			req.LeaderCommitIndex = rf.commitIndex

			go func(_serverID int, _req AppendEntryArgs, _rep AppendEntryReply) {
				ok := rf.sendAppendEntries(_serverID, &req, &rep)

				rf.Mtx.Lock()
				defer rf.Mtx.Unlock()

				if !ok || rf.CurrentState != STATE_LEADER || req.Term != rf.CurrentTerm {
					// invalid request
					return
				}
				if rep.Term > rf.CurrentTerm {
					// become follower and update current term
					log.WithFields(log.Fields{
						"by server": rf.me,
						"@ term":    rf.CurrentTerm,
					}).Warn("Stepping Down to Follower!")
					rf.CurrentTerm = rep.Term
					rf.CurrentState = STATE_FOLLOWER
					rf.VotedFor = -1
					rf.persist()
					return
				}

				if rep.Success {
					if len(req.Entries) > 0 {
						rf.NextIndex[_serverID] = req.Entries[len(req.Entries)-1].Index + 1
						rf.MatchIndex[_serverID] = rf.NextIndex[_serverID] - 1
					}
				} else {
					// the follower could have very long log..because of past leader
					// so the follower has to try couple of times with nexttryIndex
					// therefore we (the leader) has to take the minimum only
					rf.NextIndex[_serverID] = min(rep.NextTryIndex, rf.getLastLogIndex())
					// TODO
					// rf.AppendEChannel <- struct{}{} // send to all of the followers again
				}

				baseIndex := rf.Log[0].Index
				for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.Log[N-baseIndex].Term == rf.CurrentTerm; N-- {
					// find if there exists an N to update commitIndex
					count := 1
					for i := range rf.peers {
						if i != rf.me && rf.MatchIndex[i] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = N
						go rf.applyLog() // first apply then only commit
						break
					}
				}
			}(serverID, req, rep)
		}
	}
}

func (rf *Raft) Run() {
	for {
		rf.Mtx.Lock()
		_state := rf.CurrentState
		rf.Mtx.Unlock()
		switch _state {
		case STATE_FOLLOWER:
			select {
			case <-rf.GrantVoteChannel:
			case <-rf.HeartbeatChannel:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT+1)+MIN_TIMEOUT)):
				rf.Mtx.Lock()
				rf.CurrentState = STATE_CANDIDATE
				rf.Mtx.Unlock()
				rf.persist()
			}
		case STATE_LEADER:
			go rf._sendAppendEntries()
			time.Sleep(time.Millisecond * 60)
		case STATE_CANDIDATE:
			rf.Mtx.Lock()
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			var _currentTerm = rf.CurrentTerm
			var _receivedTerm = _currentTerm // _receivedTerm is going to change
			var _candidateID = rf.me
			var _nServer = len(rf.peers)
			var _LastLogIndex = rf.getLastLogIndex()
			var _LastLogTerm = rf.getLastLogTerm()
			rf.Mtx.Unlock()

			rf.persist()

			log.WithFields(log.Fields{
				"by server ": _candidateID,
				"@ term ":    _currentTerm,
			}).Info("Election started!")
			// this go routine will broadcast request vote to all the servers
			go func(__currentTerm int, __receivedTerm int, __candidateID int, __nServer int,
				__lastLogIndex int, __lastLogTerm int) {

				var localLock sync.Mutex

				var VoteChannel = make(chan bool)
				for serverID := 0; serverID < __nServer; serverID++ {
					req := RequestVoteArgs{}
					rep := RequestVoteReply{}
					req.CandidateID = __candidateID
					req.LastLogIndex = __lastLogIndex
					req.LastLogTerm = __lastLogTerm
					req.Term = __currentTerm
					if serverID != __candidateID {
						go func(_serverID int, _req RequestVoteArgs, _rep RequestVoteReply, __voteChan chan bool) {
							ok := rf.sendRequestVote(_serverID, &_req, &_rep)
							if ok {
								if _rep.Term > __currentTerm {

									localLock.Lock()
									__receivedTerm = max(__receivedTerm, _rep.Term)
									localLock.Unlock()
								}
								__voteChan <- _rep.VoteGranted
							} else {
								// log.Error("RPC failed!")
							}
						}(serverID, req, rep, VoteChannel)
					}
				}

				var VoteCnt = 1 // my vote
				var TotalCnt = 0
				var canStopWait = false
				for !canStopWait {
					select {
					case vote := <-VoteChannel:
						TotalCnt++
						if vote {
							VoteCnt++
						}
						if VoteCnt > _nServer/2 || TotalCnt == __nServer-1 {
							canStopWait = true
						}
					}
				}

				rf.Mtx.Lock()
				if __receivedTerm > _currentTerm {
					log.WithFields(log.Fields{
						"by server ":  rf.me,
						"@ old term ": _currentTerm,
					}).Warn("Higher Term received in the election")
					rf.CurrentState = STATE_FOLLOWER
					rf.CurrentTerm = __receivedTerm // this is important
					rf.VotedFor = -1
					rf.persist()
				} else {
					if rf.CurrentState == STATE_CANDIDATE && rf.CurrentTerm == _currentTerm {
						if VoteCnt > (len(rf.peers) / 2) {
							rf.CurrentState = STATE_LEADER
							log.WithFields(log.Fields{
								" server ": _candidateID,
								"@ term ":  _currentTerm,
							}).Info("Leader Elected is ")

							rf.NextIndex = make([]int, len(rf.peers))
							rf.MatchIndex = make([]int, len(rf.peers))
							nextIndex := rf.getLastLogIndex() + 1
							for i := range rf.NextIndex {
								rf.NextIndex[i] = nextIndex
							}

							rf.WinElectChannel <- true
						}
					} else {
						log.Println(__candidateID, "> Something might have happened in between!")
					}
				}
				rf.Mtx.Unlock()
			}(_currentTerm, _receivedTerm, _candidateID, _nServer, _LastLogIndex, _LastLogTerm)

			select {
			case <-rf.HeartbeatChannel:
				rf.Mtx.Lock()
				rf.CurrentState = STATE_FOLLOWER
				rf.Mtx.Unlock()
			case <-rf.WinElectChannel:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT+1)+MIN_TIMEOUT)):
			}
		}
	}
}

// Make Function is ...
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.GrantVoteChannel = make(chan bool, 1000)
	rf.WinElectChannel = make(chan bool, 1000)
	rf.HeartbeatChannel = make(chan bool, 1000)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.VoteCount = make(map[int]int)
	rf.nServer = len(peers)
	// Your initialization code here (2A, 2B, 2C).

	// Server when statrs up, starts a follower
	rf.CurrentState = STATE_FOLLOWER
	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{Term: 0, Index: 0})
	rf.VotedFor = -1
	rand.Seed(time.Now().UTC().UnixNano())

	rf.NextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		if i != me {
			rf.NextIndex[i] = 1 // this should be log last index + 1
		}
	}
	rf.MatchIndex = make([]int, len(peers))
	log.SetFormatter(&log.TextFormatter{
		DisableColors:          false,
		FullTimestamp:          false,
		DisableLevelTruncation: false,
	})
	log.SetLevel(log.TraceLevel)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	go rf.Run()

	return rf
}
