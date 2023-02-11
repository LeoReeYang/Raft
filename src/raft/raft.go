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
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	log               []Entry
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	status            Status
	applyCh           chan ApplyMsg
	validAccess       bool
	lastIncludedIndex int
	lastIncludedTerm  int
	electionTicker    *time.Ticker
	heartbeatTicker   *time.Ticker
	snapshotTicker    *time.Ticker
	applyCond         *sync.Cond
	applyChan         chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raft := w.Bytes()
	rf.persister.SaveRaftState(raft)
}

// restore previously persisted state.
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []Entry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		return
	} else {
		rf.log = log
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	actual := rf.actual(index)
	term := rf.log[actual].Term
	copy := clone(snapshot)
	rf.persister.snapshot = copy

	// Debug("\t\t%d original log = %v, last = %d, acutal = %d\n", rf.me, rf.log, rf.lastLogIndex(), rf.actual(index+1))
	Debug(dSnap, "S%d trime at %d, last = %d, log = %v", rf.me, index, rf.lastLogIndex(), rf.log)

	log := make([]Entry, 1)
	log = append(log, rf.log[actual+1:]...)
	rf.log = log

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	go func(term, index int, snapshot []byte) {
		rf.applyCh <- *snapshotMsg(snapshot, term, index)
	}(term, index, copy)

	// Debug("\t\t%v called by Snapshot, lastIncludeIndex = %d term = %d, \n\t\ttrimmed log :%v", rf.me, index, term, rf.log)
	Debug(dSnap, "S%d lastInclude = %d, trimmed log = %v", rf.me, rf.lastIncludedIndex, rf.log)
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm || args.LastIncludedTerm < rf.lastIncludedTerm || args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	copy := clone(args.Data)
	rf.persister.snapshot = copy
	rf.validAccess = true

	rf.convertRole(args.Term)

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm

	log := make([]Entry, 1)
	if snapshotIndex < rf.logical(rf.lastLogIndex()) {
		log = append(log, rf.log[rf.actual(snapshotIndex+1):]...)
	}
	rf.log = log

	rf.lastIncludedIndex = snapshotIndex
	rf.lastIncludedTerm = snapshotTerm

	if snapshotIndex > rf.commitIndex {
		rf.commitIndex = snapshotIndex
	}
	if snapshotIndex > rf.lastApplied {
		rf.lastApplied = snapshotIndex
	}
	reply.Success = true

	Debug(dSnap, "S%d install snapshot, lastindex = %d, applied = %d, commmit = %d, log = %v",
		rf.me, rf.lastIncludedIndex, rf.lastApplied, rf.commitIndex, rf.log)

	// go func() {
	// 	rf.applyCh <- *snapshotMsg(copy, snapshotTerm, snapshotIndex)
	// }()

	rf.mu.Unlock()
	rf.applyCh <- *snapshotMsg(copy, snapshotTerm, snapshotIndex)
	rf.mu.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.convertRole(args.Term)

	if (rf.votedFor == Null || rf.votedFor == args.CandidateId) &&
		rf.candidateNewer(args.LastLogTerm, args.LastLogIndex) {

		rf.validAccess = true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		Debug(dVote, "S%d votedFor = %d, term = %d", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	Debug(dVote, "S%d not vote, term = %d, votedFor = %d, log = %v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []Entry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.currentTerm

	// ignore the old term vote request
	if args.Term < rf.currentTerm {
		return
	}
	// we should check the log now...maybe it will be a outdate log...
	// consider a problem, when we had a fake leader AE, we can't make this AE or hb valied unless it was a up-to-date log leader.
	// when a voteFor rpc delaied , the disconnect peer back with a election timer fires, then got the delay rpc voteFor, peer becomes
	// fake leader(not up-to-date log to anyone), then the AE will stop the election timer fires, we will never had a true leader...

	// rf.validAccess = true(we can't flash the validAccess here before we check the log is more up-to-date...)

	// we should check both the term & log, not just term higher make this AE or hb a valid rpc...
	rf.convertRole(args.Term)

	// if args.PrevLogTerm < rf.lastIncludedTerm || args.PrevLogIndex < rf.commitIndex || args.PrevLogIndex < rf.lastApplied {
	// 	return
	// }
	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}

	rf.validAccess = true
	if !rf.containLog(args.PrevLogTerm, args.PrevLogIndex) {
		return
	}

	reply.Success = true

	rf.log = rf.truncateLog(args.PrevLogIndex + 1)
	rf.log = append(rf.log, args.Entries...)

	Debug(dLog, "S%d after update, log %v", rf.me, rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.logical(rf.lastLogIndex()), args.LeaderCommit)
		rf.applyCond.Signal()
		Debug(dCommit, "S%d follower update commit = %d, lastApplied = %d",
			rf.me, rf.commitIndex, rf.lastApplied)
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		return index, rf.currentTerm, false
	}

	index = rf.appendLog(command)
	rf.persist()
	Debug(dPersist, "S%d leader write log { %d %v }, index = %d persisted...\n", rf.me, rf.currentTerm, command, index)
	term = rf.currentTerm

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.status = Candidate
	rf.votedFor = rf.me
	rf.persist()
	args := rf.requestVoteArgs()
	Debug(dVote, "S%d starts a new election, term = %d, log = %v",
		rf.me, rf.currentTerm, rf.log)
	rf.mu.Unlock()

	votesChan := make(chan bool, len(rf.peers))
	for id := range rf.peers {
		if id == rf.me {
			votesChan <- true
			continue
		}

		go func(serverID int) {
			reply := &RequestVoteReply{}

			if ok := rf.sendRequestVote(serverID, args, reply); !ok {
				Debug(dError, "S%d Candidate send RequestVote -> %d failed, term = %d", args.CandidateId, serverID, args.Term)
				votesChan <- false
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()

			votesChan <- reply.VoteGranted

			if rf.convertRole(reply.Term) {
				return
			}
		}(id)
	}

	iter := 0
	voteCnt := 0

	for vote := range votesChan {
		iter++
		if vote {
			voteCnt++
		}

		rf.mu.Lock()
		if rf.status == Candidate {
			if voteCnt > len(rf.peers)/2 {
				rf.status = Leader
				Debug(dLeader, "S%d becomes Leader, term = %d", rf.me, rf.currentTerm)
				rf.nextInitial()
				rf.matchInitial()
				rf.mu.Unlock()
				return
			}
			if voteCnt+len(rf.peers)-iter < (len(rf.peers)+1)/2 {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) heartbeat() {
	term := rf.GetCurrentTerm()
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(serverId int) {
			args := rf.AppendEntriesArgs(term, serverId)
			reply := &AppendEntriesReply{}

			if ok := rf.sendAppendEntries(serverId, args, reply); !ok {
				return
			}

			rf.mu.Lock()
			if rf.convertRole(reply.Term) {
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Success {
				rf.UpdateIndex(serverId, args.PrevLogIndex+1+len(args.Entries))
				rf.UpdateCommit()
			} else {
				rf.DecreaseNext(serverId)
			}
		}(id)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.electionTicker.C:
			rf.electionTicker.Reset(GetRandomTime())
			if rf.CanElect() {
				go rf.startElection()
			}
		case <-rf.heartbeatTicker.C:
			if rf.GetStatus() == Leader {
				go rf.heartbeat()
			}
		case <-rf.snapshotTicker.C:
			if rf.GetStatus() == Leader {
				go rf.snapshot()
			}
		}
	}
}

func (rf *Raft) applyLogs() {
	rf.applyCond.L.Lock()
	go func() {
		for {
			rf.applyCond.Wait()
			for rf.lastApplied+1 <= rf.commitIndex {
				if !rf.applyLog(rf.lastApplied + 1) {
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					rf.mu.Lock()
					continue
				}
				rf.lastApplied++
			}
		}
	}()
}

func (rf *Raft) applyLog(applyid int) bool {
	msg := rf.applyMsg(applyid)

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case rf.applyCh <- msg:
		Debug(dApply, "S%d apply a msg ok, term = %d, applyLog = %v, applyIndex = %d",
			rf.me, rf.currentTerm, rf.log[rf.actual(msg.CommandIndex)], msg.CommandIndex)
		return true
	case <-timer.C:
		Debug(dError, "S%d apply msg: %v timeout...", rf.me, msg)
		return false
	}
}

func (rf *Raft) snapshot() {

	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		id := id
		go func() {
			args := rf.InstallSnapshotArgs()
			Debug(dSnap, "S%d try send snapshot -> %d, lastIndex = %d", rf.me, id, args.LastIncludedIndex)

			reply := &InstallSnapshotReply{}
			if ok := rf.sendInstallSnapshot(id, args, reply); !ok {
				Debug(dError, "S%d send snapshot -> %d failed...", rf.me, id)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()

			if reply.Success {
				Debug(dSnap, "S%d send snapshot -> %d ok, update match = %d, next = %d", rf.me, id, rf.lastIncludedIndex, rf.lastIncludedIndex+1)
				rf.matchIndex[id] = rf.lastIncludedIndex
				rf.nextIndex[id] = rf.lastIncludedIndex + 1
			}

			rf.convertRole(reply.Term)
		}()
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = Null
	rf.log = make([]Entry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.status = Follower
	rf.applyCh = applyCh
	rf.electionTicker = time.NewTicker(GetRandomTime())
	rf.heartbeatTicker = time.NewTicker(100 * time.Millisecond)
	rf.snapshotTicker = time.NewTicker(150 * time.Millisecond)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyChan = make(chan int, 512)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	// rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.persister.snapshot = rf.persister.ReadSnapshot()
	Debug(dPersist, "S%d read Persist state, Term = %d, VotedFor = %d, lastapplied = %d, Log = %v",
		rf.me, rf.currentTerm, rf.votedFor, rf.lastApplied, rf.log)
	// start ticker goroutine to start elections
	// rf.mu.Unlock()

	go rf.ticker()

	go rf.applyLogs()

	return rf
}
