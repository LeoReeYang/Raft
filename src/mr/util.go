package mr

import "time"

type Phrase int
type TaskType int
type TaskStatus int
type RegisterStatus int

const (
	MaxRunTime = 10 * time.Second
)

const (
	Mapping Phrase = iota
	Reducing
	Done
)

const (
	Success RegisterStatus = iota
	Failure
)

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

const (
	Available TaskStatus = iota
	Handling
	Finished
)

type Task struct {
	TaskID   int
	TaskType TaskType
	FileName string
}

type TaskDetail struct {
	taskid       int
	status       TaskStatus
	assignedTime time.Time
}

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
