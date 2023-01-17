package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	RWMutex sync.RWMutex
	nReduce int
	mSplit  int
	phrase  Phrase

	Tasks       map[int]string
	MapTasks    map[int]TaskDetail
	ReduceTasks map[int]TaskDetail
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		Tasks:       make(map[int]string),
		MapTasks:    make(map[int]TaskDetail),
		ReduceTasks: make(map[int]TaskDetail),
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	reply.Status = Success
	reply.Nreduce = c.nReduce
	reply.MSplit = c.mSplit
	return nil
}

func (c *Coordinator) GetTask(args *TaskAskArgs, reply *TaskAskReply) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	if c.phrase == Mapping {
		for id, detail := range c.MapTasks {
			if detail.status == Available ||
				detail.status == Handling && detail.assignedTime.Add(MaxRunTime).Before(time.Now()) {
				reply.Task.FileName = c.Tasks[id]
				reply.Task.TaskID = id
				reply.Task.TaskType = Map

				c.MapTasks[id] = TaskDetail{taskid: id, status: Handling, assignedTime: time.Now()}

				return nil
			}
		}
		reply.Task.TaskType = Wait
		return nil
	} else if c.phrase == Reducing {
		for id, detail := range c.ReduceTasks {
			if detail.status == Available ||
				detail.status == Handling && detail.assignedTime.Add(MaxRunTime).Before(time.Now()) {
				reply.Task.FileName = fmt.Sprintf("mr-out-%d", id)
				reply.Task.TaskID = id
				reply.Task.TaskType = Reduce

				c.ReduceTasks[id] = TaskDetail{taskid: id, status: Handling, assignedTime: time.Now()}

				return nil
			}
		}
		reply.Task.TaskType = Wait
		return nil
	}
	reply.Task.TaskType = Exit
	return nil
}

func (c *Coordinator) FinishTask(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	reply.Status = 0

	if args.TaskType == Map {
		detail := c.MapTasks[args.TaskId]
		detail.status = Finished
		c.MapTasks[args.TaskId] = detail

		finished := true

		for _, detail := range c.MapTasks {
			if detail.status != Finished {
				finished = false
				break
			}
		}

		if finished {
			c.phrase = Reducing
		}
		return nil
	} else if args.TaskType == Reduce {
		detail := c.ReduceTasks[args.TaskId]
		detail.status = Finished
		c.ReduceTasks[args.TaskId] = detail

		finished := true

		for _, detail := range c.ReduceTasks {
			if detail.status != Finished {
				finished = false
				break
			}
		}

		if finished {
			c.phrase = Done
		}
		return nil
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()

	if c.phrase == Done {
		return true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator()

	// Your code here.

	c.mSplit = len(files)
	c.nReduce = nReduce
	c.phrase = Mapping

	for i := 0; i < c.mSplit; i++ {
		c.Tasks[i] = files[i]
		c.MapTasks[i] = TaskDetail{taskid: i, status: Available, assignedTime: time.Now()}
	}

	for j := 0; j < c.nReduce; j++ {
		c.ReduceTasks[j] = TaskDetail{taskid: j, status: Available, assignedTime: time.Now()}
	}

	c.server()
	return c
}
