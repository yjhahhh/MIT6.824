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

const MapTask int = 1
const ReduceTask int = 2
const NOTask int = 3
// 任务结束
const Finished int = 4

//任务
type Task struct {
	// map任务还是reduce任务 map:0 reduce:1
	TaskType int
	FT FileTask
}


// 任务执行情况
type TaskState struct {
	TaskType int
	No int
	State bool
}

const Continue int = 1
const Finish int = -1

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkArgs struct {
	// 暂时无用
	X int
}

type WorkReply struct {
	WorkTask Task
	NReduce int
	NMap int
}

type ReportArgs struct {
	TS TaskState
}

type ReportReply struct {
	Reply int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
