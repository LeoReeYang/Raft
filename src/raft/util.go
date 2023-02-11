package raft

import (
	"math/rand"
	"time"
)

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

const (
	BaseIntervalTime int = 650
	RandomTimeValue  int = 150
	Null             int = -1
)

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

	Debug(dVote, "S%d compare log, lastlogTerm = %d, index = %d;candidate: T = %d, idx = %d",
		rf.me, myLastTerm, rf.logical(rf.lastLogIndex()), lastLogTerm, lastLogIndex)

	return lastLogTerm > myLastTerm || lastLogTerm == myLastTerm && lastLogIndex >= rf.logical(rf.lastLogIndex())
}

func (rf *Raft) containLog(term, index int) bool {
	lastLogIndex := rf.logical(rf.lastLogIndex())
	Debug(dLog, "S%d check target = %d, lastInclude = %d, actual = %d, lastlogindex = %d, log = %v",
		rf.me, index, rf.lastIncludedIndex, rf.actual(index), rf.logical(rf.lastLogIndex()), rf.log)

	if index < rf.lastIncludedIndex {
		return false
	}

	if index == rf.lastIncludedIndex {
		return true
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
	entries := make([]Entry, 0)

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	// for follower fall so far behind, send a heartbeat
	// if actual < 1 {
	// 	args.Entries = entries
	// 	args.PrevLogIndex = rf.logical(rf.lastLogIndex())
	// 	args.PrevLogTerm = rf.log[rf.lastLogIndex()].Term

	// 	Debug(dLog, "S%d leader heartbeat to %d, due to fall behind..., next = %d, lastInclude = %d, actual = %d",
	// 		rf.me, id, rf.nextIndex[id], rf.lastIncludedIndex, actual)
	// 	return args
	// }

	if actual < 1 {
		entries = append(entries, rf.log[1:]...)
		args.Entries = entries
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		Debug(dLog, "S%d leader heartbeat -> %d, due to fall behind..., next = %d, lastInclude = %d, actual = %d, log = %v",
			rf.me, id, rf.nextIndex[id], rf.lastIncludedIndex, actual, entries)
		return args
	}

	entries = append(entries, rf.log[actual:]...)
	args.Entries = entries
	args.PrevLogIndex = next - 1
	args.PrevLogTerm = rf.log[actual-1].Term
	Debug(dLog2, "S%d leader copy log -> %d, PrevId = %d, PrevT = %d, log = %v", rf.me, id, next-1, rf.log[actual-1].Term, entries)
	return args
}

func (rf *Raft) applyMsg(applyId int) ApplyMsg {
	Debug(dApply, "S%d try to apply index = %d, commitIndex = %d, lastApplied = %d, lastInclude = %d",
		rf.me, applyId, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
	return ApplyMsg{
		CommandValid: true,
		Command:      rf.log[rf.actual(applyId)].Command,
		CommandIndex: applyId,
	}
}

func (rf *Raft) UpdateIndex(id, next int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[id] = next
	rf.matchIndex[id] = next - 1
	// DPrintf("leader %v update peer %v's nextxIndex = %v, CurrentTerm= %v\n",
	// 	rf.me, id, nextIndex, rf.currentTerm)
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
				Debug(dCommit, "S%d leader update commit = %v, lastApplyId = %d",
					rf.me, rf.commitIndex, rf.lastApplied)
				return
			}
		}
	}
}

func (rf *Raft) DecreaseNext(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.matchIndex[id] == 0 {
		rf.nextIndex[id] = rf.lastIncludedIndex + 1
	} else {
		rf.nextIndex[id] = rf.nextIndex[id] - (rf.nextIndex[id]-rf.matchIndex[id])/2 - 1
		if rf.nextIndex[id] <= rf.matchIndex[id] {
			rf.nextIndex[id] = rf.matchIndex[id] + 1
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

func (rf *Raft) CanElect() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	shouldElection := !rf.validAccess && rf.status != Leader
	rf.validAccess = false
	return shouldElection
}

func (rf *Raft) logical(index int) int {
	return index + rf.lastIncludedIndex
}

func (rf *Raft) actual(index int) int {
	return index - rf.lastIncludedIndex
}

func snapshotMsg(snapshot []byte, term, index int) *ApplyMsg {
	return &ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  term,
		SnapshotIndex: index,
	}
}

func (rf *Raft) InstallSnapshotArgs() *InstallSnapshotArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
}
