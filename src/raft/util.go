package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

const (
	BaseIntervalTime int = 650
	RandomTimeValue  int = 150
)

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

func GetRandomTime() time.Duration {
	return time.Duration(BaseIntervalTime+rand.Intn(RandomTimeValue)) * time.Millisecond
}

func (rf *Raft) updateStatus(status Status) {
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

func (rf *Raft) CandidateIsNewerLog(term, index int) bool {
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if term > lastLogTerm || term == lastLogTerm && index >= len(rf.log)-1 {
		return true
	}
	return false
}

func (rf *Raft) ResetElectionTimer() {
	random := GetRandomTime()
	if rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(random)
}

func (rf *Raft) InitialTimer() *time.Timer {
	random := GetRandomTime()
	return time.NewTimer(random)
}

func (rf *Raft) RequestVoteArgs() *RequestVoteArgs {
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
