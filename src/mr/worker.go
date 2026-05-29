package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
			if err := ExecMapTask(task, mapf); err == nil {
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

func ExecReduceTask(reduceId int, reducef func(string, []string) string) error {
	log.Printf("ExecReduceTask %d", reduceId)

	intermediate := make([]KeyValue, 0)
	files, err := os.ReadDir(".")
	if err != nil {
		log.Printf("Failed to read directory: %v", err)
		return err
	}

	reduceRe := regexp.MustCompile(`^mr-[0-9]+-([0-9]+)$`)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		matches := reduceRe.FindStringSubmatch(file.Name())
		if matches == nil {
			continue
		}

		fileReduceId, err := strconv.Atoi(matches[1])
		if err != nil {
			log.Printf("Failed to parse reduce ID from %s: %v", file.Name(), err)
			continue
		}
		if fileReduceId != reduceId {
			continue
		}

		content, err := os.ReadFile(file.Name())
		if err != nil {
			log.Printf("Failed to read intermediate file %s: %v", file.Name(), err)
			return err
		}

		scanner := bufio.NewScanner(strings.NewReader(string(content)))
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, " ", 2)
			if len(parts) != 2 {
				continue
			}
			intermediate = append(intermediate, KeyValue{Key: parts[0], Value: parts[1]})
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Failed to scan intermediate file %s: %v", file.Name(), err)
			return err
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	outFileName := fmt.Sprintf("mr-out-%d", reduceId)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Printf("Failed to create output file %s: %v", outFileName, err)
		return err
	}
	defer outFile.Close()

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(outFile, "%s %s\n", intermediate[i].Key, output); err != nil {
			log.Printf("Failed to write reduce output to %s: %v", outFileName, err)
			return err
		}

		i = j
	}

	return nil
}

func ExecMapTask(task *Task, mapf func(string, string) []KeyValue) error {
	log.Printf("TaskId %v ExecMapTask %s", task.Id, task.Filename)

	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Printf("Failed to read file %s: %v", task.Filename, err)
		return err
	}

	buckets := make([][]KeyValue, task.NReduce)

	kvList := mapf(task.Filename, string(content))
	for _, kv := range kvList {
		reduceId := ihash(kv.Key) % task.NReduce
		buckets[reduceId] = append(buckets[reduceId], kv)
	}

	for i, bucket := range buckets {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.Id, i)

		// create tmp file at curr dir
		f, err := os.Create(intermediateFileName)
		if err != nil {
			log.Printf("Failed to create intermediate file %s: %v", intermediateFileName, err)
			return err
		}
		defer f.Close()

		for _, kv := range bucket {
			_, err := fmt.Fprintf(f, "%s %s\n", kv.Key, kv.Value)
			if err != nil {
				log.Printf("Failed to write to intermediate file %s: %v", intermediateFileName, err)
				return err
			}
		}
	}

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
