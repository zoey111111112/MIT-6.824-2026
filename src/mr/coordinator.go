package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

/*
MapReduce 任务整体流程：

	Coordinator：
		- MakeCoordinator(sockname, files, nReduce) 为每个输入文件创建一个
		  map task，并创建 nReduce 个 reduce task。
		- 每个 task 至少需要记录 id、status 和 start time；map task 还需要
		  记录自己的输入文件名。
		- Worker 会通过 RPC 反复向 coordinator 请求任务。
		- Coordinator 会先调度所有 map task。只有当全部 map task 完成后，
		  才能开始调度 reduce task。
		- 如果某个 task 已经被分配，但大约 10 秒后仍未完成，coordinator
		  应该把它重新变为可分配状态，让其他 worker 重试。
		- Done() 只有在全部 reduce task 都完成后才返回 true。

	Map 阶段：
		- Worker 收到一个 map task，其中包含 map task id X 和输入文件名。
		- Worker 读取输入文件，并调用 mapf(filename, content)，得到一组
		  key/value pairs，例如：
		      [("tom", "1"), ("amy", "1"), ("tom", "1")]
		- Worker 使用 ihash(key) % nReduce，把这些 key/value pairs 分到
		  nReduce 个 bucket 中。
		- 对每个 reduce bucket Y，worker 写出一个中间文件：
		      mr-X-Y
		  其中 X 是 map task id，Y 是 reduce task id。
		- 如果有 nMap 个输入文件和 nReduce 个 reduce task，那么 map 阶段
		  一共会生成 nMap * nReduce 个中间文件。
		- 当 worker 写完 map task X 对应的全部中间文件后，它会向
		  coordinator 报告该 task 已完成。

	Reduce 阶段：
		- Worker 收到一个 reduce task，其中包含 reduce task id Y。
		- Worker 读取属于这个 reduce bucket 的所有中间文件：
		      mr-0-Y, mr-1-Y, ..., mr-(nMap-1)-Y
		- Worker 合并所有 key/value pairs，按 key 排序，把相同 key 的 values
		  聚合到一起，然后调用 reducef(key, values)，例如：
		      [("amy", ["1"]), ("tom", ["1", "1"])]
		- reduce task Y 的输出写入：
		      mr-out-Y
		  每个 key 输出一行，格式为 "%v %v\n"。
		- 当 worker 写完 mr-out-Y 后，它会向 coordinator 报告该 reduce task
		  已完成。

	Worker 生命周期：
		- Worker 不断循环：请求任务、执行任务、报告完成。
		- 如果当前没有可分配的任务，coordinator 可以让 worker 等一会儿后
		  再次请求。
		- 当整个 job 完成后，coordinator 可以通知 worker 退出；或者 worker
		  在无法连接 coordinator RPC server 时自行退出。
*/

/*
各个组件职责：

	MakeCoordinator(sockname, files, nReduce):
		创建 map task 列表
		每个输入文件对应一个 map task
		task 状态初始为 idle

	Worker 循环:
		向 Coordinator 请求任务

	Coordinator:
		如果还有未完成的 map task:
			分配一个 idle 或超时的 map task
		如果所有 map task 完成，但 reduce 还没完成:
			分配一个 idle 或超时的 reduce task
		如果全部完成:
			告诉 worker 可以退出
		如果当前没任务可分配:
			告诉 worker wait 一会儿

	Map worker:
		读取输入文件
		执行 mapf(filename, content)
		对每个 key/value 用 ihash(key) % nReduce 分桶
		写出 nReduce 个中间文件:
			mr-mapTaskID-reduceTaskID
		完成后 RPC 通知 coordinator

	Reduce worker:
		对自己的 reduceTaskID = Y
		读取所有 mr-X-Y
		合并 key/value
		按 key 排序
		对每个 key 聚合 values
		执行 reducef(key, values)
		写入 mr-out-Y
		完成后 RPC 通知 coordinator

	Done():
		所有 reduce task 都完成后返回 true
*/

type Task struct {
	Id  int
	run func(mapf func(string, string) []KeyValue, reducef func(string, []string) string)
}

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleReq, reply *ExampleRes) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("listen error %s: %v", sockname, err)
	}

	log.Printf("Coordinator RPC server listening on %s", sockname)
	go http.Serve(listener, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server(sockname)
	return &c
}
