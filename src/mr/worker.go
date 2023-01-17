package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.

type worker struct {
	workerID int
	nReduce  int
	mSplit   int
}

func NewWorker() *worker {
	return &worker{
		workerID: os.Getpid(),
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	woker := NewWorker()
	woker.Register()

	woker.RequestTask(mapf, reducef)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func (w *worker) Register() error {
	args := RegisterArgs{WorkerID: w.workerID}
	reply := RegisterReply{}

	if ok := call("Coordinator.Register", &args, &reply); ok {
		w.nReduce = reply.Nreduce
		w.mSplit = reply.MSplit
		return nil
	} else {
		panic(errors.New("register call failed"))
	}
}

func (w *worker) RequestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskAskArgs{WorkerID: w.workerID}
		reply := TaskAskReply{}

		if ok := call("Coordinator.GetTask", &args, &reply); ok {
			task := reply.Task
			switch task.TaskType {
			case Map:
				w.HandleMapTask(task.FileName, task.TaskID, mapf)
			case Reduce:
				w.HandleReduceTask(task.TaskID, reducef)
			case Wait:
				time.Sleep(time.Duration(5 * time.Second))
			case Exit:
				log.Printf("All tasks are done.\n")
				return
			}
		}
	}
}

func (w *worker) HandleMapTask(filename string, taskid int, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	tempfiles := make([]*os.File, w.nReduce)
	fileEncoders := make([]*json.Encoder, w.nReduce)

	path, err := os.Getwd()
	if err != nil {
		log.Fatal("get current dir failed.\n", err)
	}

	for i := 0; i < w.nReduce; i++ {
		f, err := os.CreateTemp(path, fmt.Sprintf("mr-%d-%d", taskid, i)+"-*.txt")
		if err != nil {
			log.Fatal("create tempfile failed.\n", err)
		}
		tempfiles[i] = f
		fileEncoders[i] = json.NewEncoder(f)
	}

	for _, kv := range intermediate {
		hash := ihash(kv.Key) % w.nReduce
		err := fileEncoders[hash].Encode(&kv)
		if err != nil {
			log.Fatal("encode failed.\n", err)
		}
	}

	for id, file := range tempfiles {
		err := os.Rename(filepath.Join(file.Name()), fmt.Sprintf("mr-%d-%d", taskid, id))
		if err != nil {
			log.Fatal("rename failed.\n", err)
		}
		file.Close()
	}

	reply := w.FinishTask(filename, taskid, Map)
	if reply == nil {
		log.Fatal("finish task failed")
	}
}

func (w *worker) HandleReduceTask(taskid int, reducef func(string, []string) string) {
	kvs := []KeyValue{}
	for i := 0; i < w.mSplit; i++ {
		f, err := os.Open(fmt.Sprintf("mr-%d-%d", i, taskid))
		if err != nil {
			log.Fatal("open file failed.\n", err)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(kvs))

	path, err := os.Getwd()
	if err != nil {
		log.Fatal("can't create tempfile\n", err)
	}

	f, err := os.CreateTemp(path, fmt.Sprintf("mr-out-%d", taskid)+"-*.txt")
	if err != nil {
		log.Fatalf("failed create temp reduce output file.")
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		fmt.Fprintf(f, "%v %v\n", kvs[i].Key, reducef(kvs[i].Key, values))
		i = j
	}

	//	if the target file exist,new file will replace the old one
	err = os.Rename(filepath.Join(f.Name()), fmt.Sprintf("mr-out-%d", taskid))
	if err != nil {
		log.Fatalf("Rename ReduceTask failed\n")
	}

	reply := w.FinishTask(fmt.Sprintf("mr-out-%d", taskid), taskid, Reduce)
	if reply == nil {
		log.Fatal("finish task failed\n")
	}
	f.Close()
}

func (w *worker) FinishTask(fname string, taskid int, taskType TaskType) *TaskFinishReply {
	args := TaskFinishArgs{
		FileName: fname,
		TaskId:   taskid,
		TaskType: taskType,
	}
	reply := TaskFinishReply{}

	if ok := call("Coordinator.FinishTask", &args, &reply); ok {
		log.Printf("finish task:  id:%d   name:%v type:%v\n\n", taskid, fname, taskType)
		return &reply
	} else {
		log.Fatal("FinishTask call failed.\n")
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
