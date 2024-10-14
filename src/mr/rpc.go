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
type TaskArgs struct {
}

type TaskReply struct {
	TaskType     int // 0 : map; 1 : reduce; 2 : wait and query task again; 3 : task finished
	TotalReduce  int
	TotalMap     int
	MapTaskId    int
	FileName     string
	ReduceTaskId int
}

type FinishMapArgs struct {
	MapTaskId int
}

type FinishMapReply struct {
	Status int // 0 : initial; 1 : success; -1 : fail
}

type FinishReduceArgs struct {
	ReduceTaskId int
}

type FinishReduceReply struct {
	Status int // 0 : initial; 1 : success; -1 : fail
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
