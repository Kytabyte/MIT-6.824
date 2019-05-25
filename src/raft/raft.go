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

// import "fmt"
import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	applyCh     chan ApplyMsg
	state       State
	currentTerm int
	votedFor    int
	log         []interface{}

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other state Raft server must maintain
	electionTimeout int
	heartbeatCh     chan int
	requestVoteCh   chan int
}

// State struct indicates a Raft's state
type State int

const (
	follower  State = iota
	candidate State = iota
	leader    State = iota
)

const (
	electionTimeoutLower  int = 400
	electionTimeoutUpper  int = 2300
	sendHeartbeatInterval int = 100
	sendRequestVoteRetry  int = 100
)

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == leader)
	
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(rf.me, rf.currentTerm, args.Term, args.CandidateID)
	if (args.Term < rf.currentTerm) || (args.Term == rf.currentTerm && args.CandidateID != rf.votedFor && -1 != rf.votedFor) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		go func() {rf.requestVoteCh <- 1}()
	}
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
func (rf *Raft) doSendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(successCh chan bool) {
	var agree int
	var mu sync.Mutex

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateID: rf.me}
			reply := &RequestVoteReply{}

			for ok := rf.doSendRequestVote(server, args, reply); !ok; {
				time.Sleep(time.Millisecond * time.Duration(sendRequestVoteRetry))
			}
			if reply.VoteGranted {
				mu.Lock()
				agree++
				if agree == len(rf.peers) / 2 {
					go func() { close(successCh) }()
				}
				mu.Unlock()
			}
		}(i)
	}
}

// AppendEntryArgs contains the args sent to Raft.AppendEntries
type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

// AppendEntryReply contains the args received from Raft.AppendEntries
type AppendEntryReply struct {
	Term    int
	Success bool
}

// AppendEntries reply the current term of Raft and if the append entry operation
// is successful.
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true

		rf.currentTerm = reply.Term
		if len(args.Entries) == 0 {
			go func() {rf.heartbeatCh <- 1}()
		}
	}
}

func (rf *Raft) doSendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntryArgs{
				Term:     rf.currentTerm,
				LeaderID: rf.me}
			reply := &AppendEntryReply{}

			for {
				ok := rf.doSendAppendEntries(server, args, reply)
				if ok && !reply.Success {
					break
				}
				time.Sleep(time.Millisecond * time.Duration(sendHeartbeatInterval))
			}
		}(i)
	}
}

// resetElectionTimeout() reset the Raft.electionTimeout field.
// Since it is designed in an unblocked code, it's caller must be arounded by a Raft.mu.Lock
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = rand.Intn(electionTimeoutUpper-electionTimeoutLower) + electionTimeoutLower
}

// becomeFollower() is the preparation of a Raft becoming follower.
func (rf *Raft) becomeFollower() {
	rf.mu.Lock()
	rf.state = follower
	rf.votedFor = -1
	rf.resetElectionTimeout()
	rf.mu.Unlock()

	go rf.monitorAsFollower()
}

// monitorAsFollower() defines a Raft's behaviour as a follower.
// A follower keeps receiving heartbeat or request vote; if not, it will become candidate.
func (rf *Raft) monitorAsFollower() {
	for {
		select {
		case <-rf.heartbeatCh:
		case <-rf.requestVoteCh:
		case <-time.After(time.Millisecond * time.Duration(rf.electionTimeout)):
			go rf.becomeCandidate()
			return
		}
	}
}

// becomeCandidate() is the preparation of a Raft becoming candidate.
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	rf.mu.Unlock()

	go rf.monitorAsCandidate()
}

// monitorAsCandidate() defines a Raft's behaviour as a candidate.
// A candidate will send request vote to other peers, three possibilities may happen:
// 1. receives majority votes and become leader;
// 2. split votes happens and re-elect;
// 3. receive heartbeat during election and become a follower
func (rf *Raft) monitorAsCandidate() {
	successCh := make(chan bool)

	go rf.sendRequestVote(successCh)
	select {
	case <-successCh:
		go rf.becomeLeader()
	case <-time.After(time.Millisecond * time.Duration(rf.electionTimeout)):
		go rf.becomeCandidate()
	case <-rf.heartbeatCh:
		go rf.becomeFollower()
	}
}

// becomeLeader() is the preparation of a Raft becoming leader.
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = leader
	rf.mu.Unlock()

	go rf.monitorAsLeader()
}

// monitorAsLeader() defines a Raft's behaviour as a leader.
// A leader will keep send heartbeat to followers, until it receives a vaild
// heartbeat from other peer.
func (rf *Raft) monitorAsLeader() {
	go rf.sendHeartbeat()
	<-rf.heartbeatCh
	go rf.becomeFollower()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh

	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.heartbeatCh = make(chan int)
	rf.requestVoteCh = make(chan int)

	// Boot Raft in another thread, don't block it
	go rf.becomeFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
