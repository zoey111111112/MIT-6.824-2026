package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
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

type Coordinator struct {
	stage      Stage
	taskPool   *list.List
	finished   map[int]struct{}
	totalTasks int
	nReduce    int
	timers     map[int]*time.Timer
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:      StageMap,
		taskPool:   list.New(),
		finished:   make(map[int]struct{}),
		totalTasks: len(files),
		nReduce:    nReduce,
		timers:     make(map[int]*time.Timer),
	}

	for i, filename := range files {
		c.taskPool.PushBack(Task{
			Id:       i,
			TaskType: TaskMap,
			Filename: filename,
			nReduce:  nReduce,
		})
	}

	c.server(sockname)
	c.runManager()
	return &c
}

type Request struct {
	message string
	payload interface{}
	replyCh chan Task
}

var reqCh chan Request = make(chan Request, 100)

// Manager thread
func (c *Coordinator) runManager() {
	go func() {
		for {
			log.Println("[Manager] Waiting for requests...")
			req := <-reqCh
			log.Printf("[Manager] Received request: %s", req.message)
			switch req.message {
			case "on_task_req":
				switch c.stage {
				case StageMap, StageReduce:
					if c.taskPool.Len() > 0 {
						// assign a task to worker
						task := c.taskPool.Remove(c.taskPool.Front()).(Task)
						log.Printf("[Manager] Assigning %s task ID=%d, remaining tasks in pool=%d",
							map[TaskType]string{TaskMap: "MAP", TaskReduce: "REDUCE"}[task.TaskType],
							task.Id, c.taskPool.Len())
						// start a timer for this task
						c.timers[task.Id] = time.AfterFunc(time.Second*10, func() {
							reqCh <- Request{
								message: "on_task_timeout",
								payload: task,
							}
						})

						req.replyCh <- task

					} else {
						// no task available, tell worker to wait
						log.Printf("[Manager] No tasks available in stage %v, sending wait signal", c.stage)
						req.replyCh <- Task{TaskType: TaskWait}
					}
				case StageDone:
					// job done, tell worker to exit
					log.Printf("[Manager] Job complete, sending exit signal to worker")
					req.replyCh <- Task{TaskType: TaskExit}
				}

			case "on_task_done":
				// update finished tasks
				task_id, ok := req.payload.(int)
				if !ok {
					log.Printf("Invalid payload for on_task_done: %v", req.payload)
					continue
				}
				c.timers[task_id].Stop()
				delete(c.timers, task_id)

				c.finished[task_id] = struct{}{}
				log.Printf("[Manager] Task %d completed, finished %d/%d tasks in stage %v",
					task_id, len(c.finished), c.totalTasks, c.stage)

				// if can go to next stage
				if len(c.finished) == c.totalTasks && c.stage == StageMap {
					c.stage = StageReduce
					c.finished = make(map[int]struct{})
					c.totalTasks = c.nReduce
					c.taskPool.Init()
					for i := 0; i < c.nReduce; i++ {
						c.taskPool.PushBack(Task{
							Id:       i,
							TaskType: TaskReduce,
						})
					}
					log.Printf("All map tasks done, move to reduce stage (nReduce=%d)", c.nReduce)

				} else if len(c.finished) == c.totalTasks && c.stage == StageReduce {
					c.stage = StageDone
					log.Printf("All reduce tasks done, job complete")
				}
			case "on_task_timeout":
				task, ok := req.payload.(Task)
				if !ok {
					log.Printf("Invalid payload for on_task_timeout: %v", req.payload)
					continue
				}
				// if task not finished, put it back to pool
				if _, ok := c.finished[task.Id]; !ok {
					log.Printf("Task %d timeout, reassigning", task.Id)
					c.taskPool.PushBack(task)
				} else {
					log.Printf("[Manager] Task %d timeout but already completed, skipping", task.Id)
				}
			case "on_check_done":
				if c.stage == StageDone {
					log.Printf("[Manager] Job is done, sending exit signal")
					req.replyCh <- Task{TaskType: TaskExit}
				} else {
					log.Printf("[Manager] Job not done yet, sending wait signal")
					req.replyCh <- Task{TaskType: TaskWait}
				}
			}
		}
	}()

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

func (c *Coordinator) Example(args *ExampleReq, reply *ExampleRes) error {
	reply.Y = args.X + 1
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	replyCh := make(chan Task)
	reqCh <- Request{
		message: "on_check_done",
		replyCh: replyCh,
	}
	task := <-replyCh
	return task.TaskType == TaskExit
}

func (c *Coordinator) TaskRequest(req *TaskRequestReq, res *TaskRequestRes) error {
	log.Printf("Received TaskRequest from worker")

	replyCh := make(chan Task)
	reqCh <- Request{
		message: "on_task_req",
		replyCh: replyCh,
	}

	task := <-replyCh
	res.Task = &task
	log.Printf("Assigned task %v to worker", task)

	return nil
}

func (c *Coordinator) TaskDone(req *TaskDoneReq, res *TaskDoneRes) error {
	log.Printf("Received TaskDone for task %d", req.TaskId)

	reqCh <- Request{
		message: "on_task_done",
		payload: req.TaskId,
	}

	return nil
}
