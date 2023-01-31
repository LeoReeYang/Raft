package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

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

func (rf *Raft) initialNextIndex() {
	for id := range rf.nextIndex {
		rf.nextIndex[id] = 1
	}
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
	myLastTerm := rf.log[len(rf.log)-1].Term
	return lastLogTerm > myLastTerm || lastLogTerm == myLastTerm && lastLogIndex >= len(rf.log)-1
}

func (rf *Raft) containLog(term, index int) bool {
	lastLogIndex := len(rf.log) - 1

	return index <= lastLogIndex && term == rf.log[index].Term
}

func (rf *Raft) appendLog(command interface{}) (index int) {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	return len(rf.log) - 1
}

func (rf *Raft) resetElectionTimer() {
	random := GetRandomTime()
	if rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(random)
}

func (rf *Raft) requestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.GetCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
}

func (rf *Raft) HeartbeatArgs() *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:     rf.GetCurrentTerm(),
		LeaderId: rf.me,
	}
}

func (rf *Raft) truncateLog(end int) []Entry {
	return rf.log[:end]
}

func (rf *Raft) AppendEntriesArgs(term, id int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIdx := rf.nextIndex[id]
	entries := rf.log[nextIdx:]
	if len(entries) > 0 {
		DPrintf("leader %v try to send to id = %v , nextIdx = %v, entries = %v\n", rf.me, id, nextIdx, entries)
	}

	return &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: nextIdx - 1,
		PrevLogTerm:  rf.log[nextIdx-1].Term,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) applyMsg(applyID int) ApplyMsg {
	return ApplyMsg{
		CommandValid: true,
		Command:      rf.log[applyID].Command,
		CommandIndex: applyID,
	}
}

func (rf *Raft) updateLastApplyID() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.apply <- rf.applyMsg(rf.lastApplied)
	}
}

func (rf *Raft) updateIndex(id, replicateIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = replicateIdx
	rf.matchIndex[id] = rf.nextIndex[id] - 1

	DPrintf("leader %v update peer %v 's nextxIndex = %v, matchIndex = %v\n", rf.me, id, replicateIdx, replicateIdx-1)

	count := 0
	commit := rf.commitIndex + 1

	for id, idx := range rf.matchIndex {
		if id == rf.me || idx >= commit {
			count++
		}
		if id != rf.me {
			DPrintf("%v's matchindex = %v, count = %v\n", id, idx, count)
		}

		if count >= (len(rf.peers)+1)/2 {
			rf.commitIndex = commit
			DPrintf("leader %d update commitIndex = %v, command = %v,\n", rf.me, rf.commitIndex, rf.log[commit])
			break
		}
	}

	rf.updateLastApplyID()
}

func (rf *Raft) DecreaseNextIndex(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	next := rf.nextIndex[id]
	match := rf.matchIndex[id]
	if next >= match+1 {
		rf.nextIndex[id] = 1
		idx := next - (next-match)/2 - 1
		if idx > 0 {
			rf.nextIndex[id] = idx
		}
	}
}

func (rf *Raft) ConvertRole(argsTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.convertRole(argsTerm)
}

func (rf *Raft) convertRole(argsTerm int) {
	if argsTerm > rf.currentTerm {
		rf.status = Follower
		rf.votedFor = Null
		rf.currentTerm = argsTerm
	}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}
