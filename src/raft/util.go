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

func (rf *Raft) candidateHasNewerLog(lastLogTerm, lastLogIndex int) bool {
	myLastTerm := rf.log[rf.lastLogIndex()].Term
	return lastLogTerm > myLastTerm || lastLogTerm == myLastTerm && lastLogIndex >= rf.getLogical(rf.lastLogIndex())
}

func (rf *Raft) containLog(term, index int) bool {
	lastLogIndex := rf.getLogical(rf.lastLogIndex())

	return index <= lastLogIndex && term == rf.log[rf.getActual(index)].Term
}

func (rf *Raft) appendLog(command interface{}) (index int) {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	return rf.getLogical(rf.lastLogIndex())
}

func (rf *Raft) requestVoteArgs() *RequestVoteArgs {
	logic := rf.getLogical(rf.lastLogIndex())
	term := rf.log[rf.lastLogIndex()].Term
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: logic,
		LastLogTerm:  term,
	}
}

func (rf *Raft) truncateLog(end int) []Entry {
	return rf.log[:rf.getActual(end)]
}

func (rf *Raft) AppendEntriesArgs(term, id int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	next := rf.nextIndex[id]
	var entries []Entry
	m := 1

	if len(rf.log) > 1 {
		m = min(rf.getActual(next), rf.lastLogIndex())
		entries = append(entries, rf.log[m:]...)
	}
	DPrintf("args entries to %d: logical index = %d, len = %d, match = %d, next = %d\n",
		id, rf.getLogical(m), len(entries), rf.matchIndex[id], rf.nextIndex[id])

	return &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: rf.getLogical(m - 1),
		PrevLogTerm:  rf.log[m-1].Term,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) applyMsg(applyID int) ApplyMsg {
	return ApplyMsg{
		CommandValid: true,
		Command:      rf.log[rf.getActual(applyID)].Command,
		CommandIndex: applyID,
	}
}

func (rf *Raft) UpdateIndex(id, replicateIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = replicateIdx
	rf.matchIndex[id] = rf.nextIndex[id] - 1
	DPrintf("leader %v update peer %v 's nextxIndex = %v, CurrentTerm= %v\n", rf.me, id, replicateIdx, rf.currentTerm)
}

func (rf *Raft) UpdateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.getLogical(rf.lastLogIndex()); i > rf.commitIndex && rf.log[rf.getActual(i)].Term == rf.currentTerm; i-- {
		count := 0
		for id, idx := range rf.matchIndex {
			if id == rf.me || idx >= i {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.applyCond.Signal()
				DPrintf("leader %d update commitIndex = %v, CurrentTerm = %d, lastApplyId = %d, lastLog = %v\n", rf.me, rf.commitIndex, rf.currentTerm, rf.lastApplied, rf.log[rf.lastLogIndex()])
				return
			}
		}
	}
}

func (rf *Raft) DecreaseNextIndex(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf.nextIndex[id] = rf.nextIndex[id] - (rf.nextIndex[id]-rf.matchIndex[id])/2 - 1
	// if rf.nextIndex[id] <= rf.matchIndex[id] {
	// 	rf.nextIndex[id] = rf.matchIndex[id] + 1
	// }
	rf.nextIndex[id] = rf.matchIndex[id] + 1
}

func (rf *Raft) ConvertRole(argsTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.convertRole(argsTerm)
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
		rf.nextIndex[id] = rf.getLogical(rf.lastLogIndex()) + 1
	}
}

func (rf *Raft) matchInitial() {
	for id := range rf.matchIndex {
		rf.matchIndex[id] = 0
	}
}

func (rf *Raft) canElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	shouldElection := !rf.validAccess && rf.status != Leader
	rf.validAccess = false
	return shouldElection
}

func (rf *Raft) getLogical(index int) int {
	return index + rf.snapShotIndex
}

func (rf *Raft) getActual(index int) int {
	return index - rf.snapShotIndex
}
