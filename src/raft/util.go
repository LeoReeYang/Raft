package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

const (
	BaseIntervalTime int = 650
	RandomTimeValue  int = 150
	Null             int = -1
)

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func GetRandomTime() time.Duration {
	return time.Duration(BaseIntervalTime+rand.Intn(RandomTimeValue)) * time.Millisecond
}

func (rf *Raft) UpdateStatus(status Status) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.status = status
}

func (rf *Raft) GetStatus() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.status
}

func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) candidateNewer(lastLogTerm, lastLogIndex int) bool {
	myLastTerm := rf.log[rf.lastLogIndex()].Term
	return lastLogTerm > myLastTerm || lastLogTerm == myLastTerm && lastLogIndex >= rf.logical(rf.lastLogIndex())
}

func (rf *Raft) containLog(term, index int) bool {
	lastLogIndex := rf.logical(rf.lastLogIndex())
	DPrintf("\t\t%d call cantainLog, index = %d, snapshotIndex = %d, actual = %d\n", rf.me, index, rf.snapshotIndex, rf.actual(index))

	if index < rf.snapshotIndex {
		return false
	}

	return index <= lastLogIndex && term == rf.log[rf.actual(index)].Term // out of range?
}

func (rf *Raft) appendLog(command interface{}) (index int) {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	return rf.logical(rf.lastLogIndex())
}

func (rf *Raft) requestVoteArgs() *RequestVoteArgs {
	index := rf.logical(rf.lastLogIndex())
	term := rf.log[rf.lastLogIndex()].Term
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  term,
	}
}

func (rf *Raft) truncateLog(end int) []Entry {
	return rf.log[:rf.actual(end)]
}

func (rf *Raft) AppendEntriesArgs(term, id int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	next := rf.nextIndex[id]
	actual := rf.actual(next)
	var entries []Entry

	if actual <= 0 {
		return nil
	}

	entries = append(entries, rf.log[actual:]...)

	DPrintf("args entries to %d: logical index = %d, len = %d, match = %d, actual next = %d\n,log = %v",
		id, next, len(entries), rf.matchIndex[id], actual, entries)
	return &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log[actual-1].Term,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) applyMsg(applyId int) ApplyMsg {
	return ApplyMsg{
		CommandValid: true,
		Command:      rf.log[rf.actual(applyId)].Command,
		CommandIndex: applyId,
	}
}

func (rf *Raft) UpdateIndex(id, nextIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = nextIndex
	rf.matchIndex[id] = nextIndex - 1
	DPrintf("leader %v update peer %v's nextxIndex = %v, CurrentTerm= %v\n",
		rf.me, id, nextIndex, rf.currentTerm)
}

func (rf *Raft) UpdateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.logical(rf.lastLogIndex()); i > rf.commitIndex && rf.log[rf.actual(i)].Term == rf.currentTerm; i-- {
		count := 0
		for id, idx := range rf.matchIndex {
			if id == rf.me || idx >= i {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.applyCond.Signal()
				DPrintf("leader %d update commitIndex = %v, CurrentTerm = %d, lastApplyId = %d, lastLog = %v\n",
					rf.me, rf.commitIndex, rf.currentTerm, rf.lastApplied, rf.log[rf.lastLogIndex()])
				return
			}
		}
	}
}

func (rf *Raft) DecreaseNextIndex(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = rf.nextIndex[id] - (rf.nextIndex[id]-rf.matchIndex[id])/2 - 1
	if rf.nextIndex[id] <= rf.matchIndex[id] {
		rf.nextIndex[id] = rf.matchIndex[id] + 1
		if rf.snapshotIndex > rf.matchIndex[id] {
			rf.nextIndex[id] = rf.snapshotIndex + 1
		}
	}

}

func (rf *Raft) convertRole(argsTerm int) bool {
	ret := false
	if argsTerm > rf.currentTerm {
		rf.status = Follower
		rf.votedFor = Null
		rf.currentTerm = argsTerm
		ret = true
	}
	return ret
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) nextInitial() {
	for id := range rf.nextIndex {
		if id == rf.me {
			continue
		}
		rf.nextIndex[id] = rf.logical(rf.lastLogIndex()) + 1
	}
}

func (rf *Raft) matchInitial() {
	for id := range rf.matchIndex {
		rf.matchIndex[id] = 0
	}
}

func (rf *Raft) canElect() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	shouldElection := !rf.validAccess && rf.status != Leader
	rf.validAccess = false
	return shouldElection
}

func (rf *Raft) logical(index int) int {
	return index + rf.snapshotIndex
}

func (rf *Raft) actual(index int) int {
	return index - rf.snapshotIndex
}

func (rf *Raft) updateLog(args *AppendEntriesArgs) {
	rf.log = rf.truncateLog(args.PrevLogIndex + 1)
	rf.log = append(rf.log, args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.logical(rf.lastLogIndex()), args.LeaderCommit)
		rf.applyCond.Signal()
		DPrintf("\t\t%v update commitIndex = %v, lastsApplyId = %d\n",
			rf.me, rf.commitIndex, rf.lastApplied)
	}
}
