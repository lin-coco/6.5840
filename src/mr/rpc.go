package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Common struct {
	Status int
}

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
type AskTaskArgs struct {
	WorkerId string
}
type AskTaskReply struct {
	Common
	// 任务类型
	Task Task
}

type ReportTaskStateArgs struct {
	// 工作者id
	WorkerId string
	// 任务类型
	Type string
	// 任务id
	Id int
	// 任务状态
	State string
	// 错误
	Err error
}
type ReportTaskStateReply struct {
	Common
}

type GetIntermediateFilesArgs struct {
	// 工作者id
	WorkerId string
	// reduce任务id
	ReduceTaskId int
	// 上一次获取到的索引处 -1
	ReceiveCount int
}
type GetIntermediateFilesReply struct {
	Common
	IntermediateFiles []string
	Sum               int
}

type CountIntermediateFilesArgs struct {
	// 工作者id
	WorkerId string
	// reduce任务id
	ReduceTaskId int
}
type CountIntermediateFilesReply struct {
	Common
	intermediateFileNum int
}

type GetNReduceArgs struct {
	// 工作者id
	WorkerId string
	// reduce任务id
	ReduceTaskId int
}
type GetNReduceReply struct {
	Common
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
