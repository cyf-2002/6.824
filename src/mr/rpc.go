package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// worker() 向协调器请求 Task
type Task struct {
	TaskType  TaskType
	TaskId    int
	ReduceNum int
	FileName  []string
}

// rpc 请求参数，没有意义，worker()只请求task信息
type TaskRequestArgs struct{}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waitting 代表此时为任务都分发完了，但是任务还没完成
	ExitTask
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
