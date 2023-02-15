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
	myTerm := rf.lastLogTerm()
	myIndex := rf.logical(rf.lastLogIndex())

	if lastLogTerm < myTerm {
		Debug(dInfo, "S%d can't vote due to smaller term, myTerm: %d, CandidateTerm: %d", rf.me, myTerm, lastLogTerm)
	}

	if lastLogIndex < myIndex {
		Debug(dInfo, "S%d can't vote due to smaller index, myIndex: %d, CandidateIndex %d", rf.me, myIndex, lastLogIndex)
	}

	return lastLogTerm > myTerm || lastLogTerm == myTerm && lastLogIndex >= myIndex
}

func (rf *Raft) containLog(term, index int) bool {
	lastLogIndex := rf.logical(rf.lastLogIndex())

	Debug(dLog, "S%d check index = %d, actual = %d; snapshot = %d, myLast = %d",
		rf.me,
		index,
		rf.actual(index),
		rf.lastIncludedIndex,
		lastLogIndex,
	)

	if index > lastLogIndex || index < rf.lastIncludedIndex {
		Debug(dInfo, "S%d not contain index = %d", rf.me, index)
		return false
	}

	ret := term == rf.getLogEntry(index).Term
	if !ret {
		Debug(dInfo, "S%d not contain, term differ original term = %d",
			rf.me, rf.log[rf.actual(index)].Term)
	}

	return ret
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

func (rf *Raft) AppendEntriesArgs(term, id int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	entries := make([]Entry, 0)

	prevId := rf.nextIndex[id] - 1

	if prevId < rf.lastIncludedIndex {
		prevId = rf.lastIncludedIndex
	}

	Debug(dInfo, "S%d try send AE entries from %d to %d -> peer %d, logStart = %d, end = %d",
		rf.me, prevId+1, rf.logical(rf.lastLogIndex()), id, rf.logical(0), rf.logical(rf.lastLogIndex()))

	entries = append(entries, rf.log[rf.actual(prevId+1):]...)

	if len(entries) == 0 {
		Debug(dInfo, "S%d sends heartbeat -> %d, idx = %d...", rf.me, id, prevId+1)
	} else {
		Debug(dInfo, "S%d copy log -> %d, startIdx = %d, end = %d", rf.me, id, prevId+1, prevId+1+len(entries))
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: prevId,
		PrevLogTerm:  rf.log[rf.actual(prevId)].Term,
		LeaderCommit: rf.commitIndex,
	}

	return args
}

func (rf *Raft) applyMsg(applyId int) ApplyMsg {
	// Debug(dApply, "S%d try to apply index = %d, commitIndex = %d, lastApplied = %d, lastInclude = %d",
	// 	rf.me, applyId, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
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
				Debug(dCommit, "S%d leader update commit = %v",
					rf.me,
					rf.commitIndex,
				)
				rf.applyCond.Signal()
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

	shouldElect := !rf.validAccess && rf.status != Leader
	rf.validAccess = false
	return shouldElect
}

func (rf *Raft) logical(index int) int {
	return index + rf.lastIncludedIndex
}

func (rf *Raft) actual(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) lastLogTerm() int {
	term := rf.log[rf.lastLogIndex()].Term
	return term
}

func snapshotMsg(snapshot []byte, term, index int) *ApplyMsg {
	return &ApplyMsg{
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
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) logicalStartIndex() int {
	return rf.logical(1)
}

func (rf *Raft) installSnapshot(snapshot []byte) {
	rf.persister.snapshot = snapshot
}

func (rf *Raft) getLogEntry(index int) Entry {
	return rf.log[rf.actual(index)]
}
