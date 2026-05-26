package mr

// RPC definitions.
//
// remember to capitalize all names.

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
