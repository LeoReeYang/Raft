package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

// Map functions return a slice of KeyValue.
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

type Task struct {
	TaskID   int
	TaskType TaskType
	FileName string
}

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

func (w *worker) Register() bool {
	args := RegisterArgs{WorkerID: w.workerID}
	reply := RegisterReply{}

	if ok := call("Coordinator.Register", &args, &reply); ok {
		w.nReduce = reply.Nreduce
		w.mSplit = reply.MSplit
		// fmt.Printf("Worker: %+v\n", w)
		// fmt.Printf("Register reply: %+v\n", reply)
		return ok
	} else {
		// log.Fatal("register call failed.\n")
		return ok
	}
}

func (w *worker) RequestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := TaskAskArgs{WorkerID: w.workerID}
	reply := TaskAskReply{}

	for {
		if ok := call("Coordinator.GetTask", &args, &reply); ok {
			// log.Printf("RequestTask : %+v\n", reply)

			task := reply.AssignedTask
			switch task.TaskType {
			case Map:
				w.HandleMapTask(task.FileName, task.TaskID, mapf)
				continue
			case Reduce:
				w.HandleReduceTask(task.TaskID, reducef)
				continue
			case Wait:
				time.Sleep(200 * time.Millisecond)
				log.Printf("All tasks are handling...will request task after 2s.\n")
				continue
			case Exit:
				log.Printf("All tasks are done...exit.\n")
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
	filesEncoder := make([]*json.Encoder, w.nReduce)

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
		filesEncoder[i] = json.NewEncoder(f)
	}

	for _, kv := range intermediate {
		hash := ihash(kv.Key) % w.nReduce
		err := filesEncoder[hash].Encode(&kv)
		if err != nil {
			log.Fatal("encode failed.\n", err)
		}
	}

	for id, file := range tempfiles {
		err := os.Rename(filepath.Join(file.Name()), fmt.Sprintf("mr-%d-%d", taskid, id))
		if err != nil {
			log.Fatal("rename failed.\n", err)
		}
	}

	// sort.Sort(ByKey(intermediate))

	// path := "/home/yu/6.824/src/main"

	reply := w.FinishTask(filename, taskid, Map)
	if reply == nil {
		log.Fatal("finish task failed")
	}
}

func (w *worker) HandleReduceTask(taskid int, reducef func(string, []string) string) {
	file := fmt.Sprintf("mr-out-%d", taskid)
	if _, err := os.Stat(file); err == nil {
		os.Remove(file)
	}

	outfile, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("can't open mr-out-file.", err)
	}

	defer outfile.Close()

	// i := 0
	for i := 0; i < w.mSplit; i++ {
		f, err := os.Open(fmt.Sprintf("mr-%d-%d", i, taskid))
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(f)
		kvs := []KeyValue{}
		for {
			var KV KeyValue
			if err := dec.Decode(&KV); err != nil {
				break
			}
			kvs = append(kvs, KV)
		}

		j := 0
		for j < len(kvs) {
			k := j + 1
			for k < len(kvs) && kvs[k].Key == kvs[j].Key {
				k++
			}

			values := []string{}

			for m := j; m < k; m++ {
				values = append(values, kvs[m].Value)
			}
			output := reducef(kvs[j].Key, values)

			_, err := fmt.Fprintf(outfile, "%v %v\n", kvs[j].Key, output)
			if err != nil {
				log.Fatal("write output file failed\n")
			}
			j = k
		}
	}
	reply := w.FinishTask(file, taskid, Reduce)
	if reply == nil {
		log.Fatal("finish reduce task failed")
	}
}

func (w *worker) FinishTask(fname string, taskid int, taskType TaskType) *TaskFinishReply {
	args := TaskFinishArgs{
		FileName: fname,
		TaskId:   taskid,
		TaskType: taskType,
	}
	reply := TaskFinishReply{}

	if ok := call("Coordinator.FinishTask", &args, &reply); ok {
		return &reply
	} else {
		log.Println("FinishTask call failed.")
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
