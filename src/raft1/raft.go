package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state Role

	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
}

type Log struct {
	Term int
}

type EllectionReply struct {
	term int
	role Role
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type RequestVoteMsg struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

type AppendEntriesMsg struct {
	args  *AppendEntries
	reply *AppendEntriesReply
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// 3. 检查是否已投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return // 本任期已投给别人
	}

	// 4. 投票
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	// 重置选举计时器
	rf.electionTimer.Reset(time.Duration(randomElectionTimeout()) * time.Millisecond)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}

func (rf *Raft) HeartBeat(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	reply.Term = term
	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	rf.state = Follower
	if args.Term > term {
		rf.votedFor = -1
	}
	// 重置选举计时器
	rf.electionTimer.Reset(time.Duration(randomElectionTimeout()) * time.Millisecond)
	// if args.Term < rf.currentTerm || len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }
	// lastIndex := len(rf.peers) - 1
	// for i, entry := range args.Entries {
	// 	if args.PrevLogIndex+i+1 <= lastIndex {
	// 		rf.log[args.PrevLogIndex+i+1] = entry
	// 	} else {
	// 		rf.log = append(rf.log, entry)
	// 	}
	// }
	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	// }
	// reply.Term = rf.currentTerm
	// reply.Success = true
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.electionTimer = time.NewTimer(time.Duration(randomElectionTimeout()) * time.Millisecond)
	rf.heartBeatTimer = time.NewTimer(100 * time.Millisecond)

	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = Candidate
				term := rf.currentTerm
				// 异步触发选举流程
				go rf.election(term)
				//reset计时器
			}
			rf.electionTimer.Reset(time.Duration(randomElectionTimeout()) * time.Millisecond)
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				term := rf.currentTerm
				go rf.handleSendHeartBeat(term)
			}
			rf.heartBeatTimer.Reset(100 * time.Millisecond)
			rf.mu.Unlock()
		}
	}
}

func randomElectionTimeout() int64 {
	return 300 + (rand.Int63() % 300)
}

// 发送心跳
// 可能会发生状态转变 : Leader -> Follower
func (rf *Raft) handleSendHeartBeat(term int) {
	heartBeat := &AppendEntries{
		Term:     term,
		LeaderId: rf.me,
		// PrevLogIndex: len(rf.log) - 1,
		// PrevLogTerm:  rf.log[len(rf.log)-1].term,
		// Entries:      []Log{},
		// LeaderCommit: rf.commitIndex,
	}
	heartBeatReply := make(chan *AppendEntriesReply, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		go func() {
			ok := rf.sendHeartBeat(index, heartBeat, reply)
			if ok {
				heartBeatReply <- reply
			} else {
				heartBeatReply <- nil
			}
		}()
	}
	replyCount := 0
	for replyCount < len(rf.peers)-1 {
		reply := <-heartBeatReply
		replyCount++
		if reply == nil {
			continue
		}
		if !reply.Success && reply.Term > term {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 检查当前任期是不是还小于回复消息的任期
			if rf.currentTerm < reply.Term {
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
			return
		}
	}
}
func (rf *Raft) election(term int) {
	args := &RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
		// LastLogIndex: len(rf.log) - 1,
		// LastLogTerm:  rf.log[len(rf.log)-1].term,
	}
	voteChan := make(chan *RequestVoteReply, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go func() {
			// rf.sendRequestVote(index, args, reply)
			// voteChan <- reply
			ok := rf.sendRequestVote(index, args, reply)
			if ok {
				voteChan <- reply
			} else {
				voteChan <- nil // 或者其他标记失败的手段
			}
		}()
	}
	voteGranted := 1
	majority := len(rf.peers)/2 + 1
	replyCount := 0
	for replyCount < len(rf.peers)-1 {
		reply := <-voteChan
		replyCount++
		if reply == nil {
			continue
		}
		if reply.VoteGranted {
			if voteGranted++; voteGranted >= majority {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 检查现在的任期是否已经过时了，如果过时了就不转换为leader了
				if rf.currentTerm > term {
					return
				}
				rf.state = Leader
				go rf.handleSendHeartBeat(rf.currentTerm)
				return
			}
		} else if reply.Term > term {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 当前选举结果作废
			if rf.currentTerm > term {
				return
			}
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			return
		}
	}
}
