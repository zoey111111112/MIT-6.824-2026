package mr

import (
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		task, ok := TaskRequest()
		if !ok {
			log.Println("Failed to get task")
			time.Sleep(time.Second)
			continue
		}

		switch task.TaskType {
		case TaskMap:
			if err := ExecMapTask(task.Filename, mapf); err == nil {
				TaskDone(task.Id)
			} else {
				log.Printf("Failed to execute MapTask %d: %v", task.Id, err)
			}
		case TaskReduce:
			if err := ExecReduceTask(task.Id, reducef); err == nil {
				TaskDone(task.Id)
			} else {
				log.Printf("Failed to execute ReduceTask %d: %v", task.Id, err)
			}
		case TaskWait:
			// sleep 1
			log.Println("Run WaitTask")
			time.Sleep(time.Second)
		case TaskExit:
			log.Println("Run ExitTask")
			os.Exit(0)
		default:
			log.Printf("Unknown task type: %v", task.TaskType)
		}
	}
}

func ExecReduceTask(i int, reducef func(string, []string) string) error {
	time.Sleep(time.Second)
	log.Printf("ExecReduceTask %d", i)
	return nil
}

func ExecMapTask(s string, mapf func(string, string) []KeyValue) error {
	log.Printf("ExecMapTask %s", s)
	return nil
}

func TaskRequest() (*Task, bool) {
	req := TaskRequestReq{}
	res := TaskRequestRes{}

	if ok := call(CoordinatorTaskRequest, &req, &res); !ok {
		log.Printf("Failed to call CoordinatorTaskRequest")
		return nil, false
	}
	return res.Task, true
}

func TaskDone(taskId int) {
	req := TaskDoneReq{TaskId: taskId}
	res := TaskDoneRes{}

	if ok := call(CoordinatorTaskDone, &req, &res); !ok {
		log.Printf("Failed to call CoordinatorTaskDone for task %d", taskId)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	req := ExampleReq{X: 99}
	res := ExampleRes{}

	// send the RPC request, wait for the reply.
	// CoordinatorExample tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	if ok := call(CoordinatorExample, &req, &res); ok {
		// res.Y should be 100.
		log.Printf("res.Y %v\n", res.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcEndpoint string, req interface{}, res interface{}) bool {
	// client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	client, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	if err := client.Call(rpcEndpoint, req, res); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
