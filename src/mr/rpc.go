package mr

// RPC definitions.
//
// remember to capitalize all names.

type TaskType int

// ══════════════════════════════════════════════════════════════════
// Enum
// ═══════════════════════════════════════════════════════════════════

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

func (t TaskType) String() string {
	switch t {
	case TaskMap:
		return "MapTask"
	case TaskReduce:
		return "ReduceTask"
	case TaskWait:
		return "WaitTask"
	case TaskExit:
		return "ExitTask"
	default:
		return "UnknownTaskType"
	}
}

type Stage int

const (
	StageMap Stage = iota
	StageReduce
	StageDone
)

// ═══════════════════════════════════════════════════════════════════
// Example
// ═══════════════════════════════════════════════════════════════════

const (
	CoordinatorExample string = "Coordinator.Example"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleReq struct {
	X int
}

type ExampleRes struct {
	Y int
}

// Add your RPC definitions here.

// ═══════════════════════════════════════════════════════════════════
// TaskRequest
// ═══════════════════════════════════════════════════════════════════

type Task struct {
	Id       int
	TaskType TaskType
	Filename string // 仅 map task 需要
	nReduce  int    // 仅 map task 需要
}

const CoordinatorTaskRequest string = "Coordinator.TaskRequest"

type TaskRequestReq struct {
}

type TaskRequestRes struct {
	Task *Task
}

// ═══════════════════════════════════════════════════════════════════
// TaskDone
// ═══════════════════════════════════════════════════════════════════

const CoordinatorTaskDone string = "Coordinator.TaskDone"

type TaskDoneReq struct {
	TaskId int
}

type TaskDoneRes struct {
}
