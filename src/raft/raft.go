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
	currentTerm     int
	votedFor        int
	log             []Entry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	snapshotIndex   int
	status          Status
	applyCh         chan ApplyMsg
	validAccess     bool
	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker
	snapshotTicker  *time.Ticker
	applyCond       *sync.Cond
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
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
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

	term := rf.log[rf.actual(index)].Term

	log := make([]Entry, 0)
	log = append(log, rf.log[rf.actual(index):]...)
	rf.log = log

	// DPrintf("%d before trime, next=%d, actual=%d 	log :%v\n", rf.me, index, actual, rf.log)
	rf.snapshotIndex = index
	rf.persister.snapshot = snapshot
	rf.persist()

	go func(term, index int) {
		// time.Sleep(100 * time.Millisecond)
		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      clone(snapshot),
			SnapshotTerm:  term,
			SnapshotIndex: index,
		}
		// rf.mu.Unlock()
		rf.applyCh <- msg
		// rf.mu.Lock()
	}(term, index)
	DPrintf("%v called by Snapshot, index = %d, log[0] = %+v, \n\t\ttrimmed log :%v", rf.me, index, rf.log[0], log)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
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

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.validAccess = true
	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}
	// DPrintf("\t\t%d's original snapshotIndex = %d\n", rf.me, rf.snapshotIndex)
	// DPrintf("\t\t%d's untrimmed log = %v\n", rf.me, rf.log)

	if rf.convertRole(args.Term) {
		rf.persist()
	}
	rf.persister.snapshot = clone(args.Data)

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm

	var log []Entry
	if snapshotIndex < rf.logical(rf.lastLogIndex()) {
		log = append(log, rf.log[rf.actual(snapshotIndex):]...)
	} else {
		log = append(log, Entry{Term: snapshotTerm})
	}
	rf.log = log

	rf.snapshotIndex = snapshotIndex
	DPrintf("\t\t%d's trimmed log = %v\n", rf.me, rf.log)

	if snapshotIndex > rf.commitIndex {
		rf.commitIndex = snapshotIndex
	}
	if snapshotIndex > rf.lastApplied {
		rf.lastApplied = snapshotIndex
	}
	reply.Success = true

	// DPrintf("\t\t%d update snapindex = %d, term = %d,\nlog = %v", rf.me, snapshotIndex, snapshotTerm, rf.log)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      clone(args.Data),
		SnapshotTerm:  snapshotTerm,
		SnapshotIndex: snapshotIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.convertRole(args.Term) {
		rf.persist()
	}

	if (rf.votedFor == Null || rf.votedFor == args.CandidateId) &&
		rf.candidateNewer(args.LastLogTerm, args.LastLogIndex) {

		rf.validAccess = true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		DPrintf("\t\t%v votedFor = %v, term = %d, persisted...\n", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	DPrintf("\t\t%d not vote, term = %d, votedFor = %d, lastLog[term = %v, index = %v],\nlog = %v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log[rf.lastLogIndex()].Term, rf.logical(rf.lastLogIndex()), rf.log)
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

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.validAccess = true

	if rf.convertRole(args.Term) {
		rf.persist()
	}

	if !rf.containLog(args.PrevLogTerm, args.PrevLogIndex) {
		return
	}

	reply.Success = true
	rf.updateLog(args)
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
	// DPrintf("%v write cmd = %v to local log persisted...\n", rf.me, command)
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
	DPrintf("\t\t%v starts a new election, term = %v, lastLog[term = %v, index = %v], persisted...\n",
		rf.me, rf.currentTerm, rf.log[rf.lastLogIndex()].Term, rf.logical(rf.lastLogIndex()))
	rf.mu.Unlock()

	votesChan := make(chan bool, len(rf.peers))
	for id := range rf.peers {
		if id == rf.me {
			votesChan <- true
			continue
		}

		go func(serverID int) {
			reply := RequestVoteReply{}

			if ok := rf.sendRequestVote(serverID, args, &reply); !ok {
				DPrintf("\t\tCandidate %v send RequestVote to %v failed, term = %d\n", args.CandidateId, serverID, args.Term)
				votesChan <- false
				return
			}

			rf.mu.Lock()
			if rf.convertRole(reply.Term) {
				rf.persist()
				// DPrintf("\t\t%v change term = %v, due to higher Term during election\n", rf.me, reply.Term)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			votesChan <- reply.VoteGranted
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
			if args == nil {
				return
			}
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
				rf.DecreaseNextIndex(serverId)
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
			if rf.canElect() {
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
			DPrintf("\t\t\t%d applied = %d, commit = %d\n", rf.me, rf.lastApplied, rf.commitIndex)
			for rf.lastApplied < rf.commitIndex {
				if !rf.applyLog(rf.lastApplied + 1) {
					rf.mu.Unlock()
					time.Sleep(200 * time.Millisecond)
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
		DPrintf("\t\t%v apply a msg ,term = %d, applyLog = %v, applyIndx = %d\n",
			rf.me, rf.currentTerm, rf.log[rf.actual(msg.CommandIndex)], msg.CommandIndex)
		return true
	case <-timer.C:
		DPrintf("\t\t\t%v can't apply msg :%+v...\n", rf.me, msg)
		return false
	}
}

func (rf *Raft) snapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshot := rf.persister.ReadSnapshot()
	index := rf.snapshotIndex
	term := rf.log[0].Term

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              snapshot,
	}

	for id := range rf.nextIndex {
		if id == rf.me {
			continue
		}
		id := id
		go func() {
			reply := &InstallSnapshotReply{}
			DPrintf("\t\t%d send installsnapshot to %d, lastIndex = %d, term = %d\n", rf.me, id, index, term)
			if ok := rf.sendInstallSnapshot(id, args, reply); !ok {
				DPrintf("leader %d send installsnapshot to %d failed\n", rf.me, id)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.convertRole(reply.Term) {
				rf.persist()
				return
			}

			if reply.Success {
				rf.matchIndex[id] = index
				rf.nextIndex[id] = index + 1
				DPrintf("\t\t%d install snapshot ok, next = %d, match = %d\n", id, index+1, index)
			}
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
	rf.snapshotTicker = time.NewTicker(250 * time.Millisecond)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapshotIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persister.snapshot = rf.persister.ReadSnapshot()
	// DPrintf("%v read Persist state,Term = %v, VotedFor = %v, Log = %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLogs()

	return rf
}
