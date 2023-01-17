package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RegisterArgs struct {
	WorkerID int
}
type RegisterReply struct {
	Status  RegisterStatus
	Nreduce int
	MSplit  int
}
type TaskAskArgs struct {
	WorkerID int
}
type TaskAskReply struct {
	Task Task
}

type TaskFinishArgs struct {
	FileName string
	TaskId   int
	TaskType TaskType
}

type TaskFinishReply struct {
	Status int // 0 ok, 1 failed
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
