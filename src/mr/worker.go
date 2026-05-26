package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	job := TaskRequest()
	job.run(mapf, reducef)
	TaskDone(job.Id)
}

func TaskRequest() *Task {
	// Your code here to request a task from the coordinator.
	return nil
}

func TaskDone(taskId int) {
	// Your code here to notify the coordinator that
	// the task with taskId has been completed.
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
		fmt.Printf("res.Y %v\n", res.Y)
	} else {
		fmt.Printf("call failed!\n")
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
