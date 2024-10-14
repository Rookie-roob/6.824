package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	files           []string
	total_reduce    int
	total_map       int
	finished_map    int
	finished_reduce int
	map_log_pipe    []int //0 : initial; 1 : in_progress; 2 : finished
	reduce_log_pipe []int //0 : initial; 1 : in_progress; 2 : finished
	mu              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
	m.mu.Lock()
	m.map_log_pipe[args.MapTaskId] = 2
	m.finished_map++
	m.mu.Unlock()
	reply.Status = 1
	return nil
}

func (m *Master) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {
	m.mu.Lock()
	m.reduce_log_pipe[args.ReduceTaskId] = 2
	m.finished_reduce++
	m.mu.Unlock()
	reply.Status = 1
	return nil
}

func (m *Master) AllocateTask(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	if m.finished_map < m.total_map {
		//scan if map_log_pipe has 0 item
		allocated_idx := -1
		for idx, map_item := range m.map_log_pipe {
			if map_item == 0 {
				allocated_idx = idx
				break
			}
		}
		if allocated_idx == -1 {
			m.mu.Unlock()
			reply.TaskType = 2
		} else {
			m.map_log_pipe[allocated_idx] = 1
			m.mu.Unlock()
			reply.FileName = m.files[allocated_idx]
			reply.MapTaskId = allocated_idx
			reply.TaskType = 0
			reply.TotalReduce = m.total_reduce
			go func(idx int) {
				time.Sleep(10 * time.Second)
				m.mu.Lock()
				if m.map_log_pipe[idx] == 1 {
					m.map_log_pipe[idx] = 0
				}
				m.mu.Unlock()
			}(allocated_idx)
		}
	} else {
		if m.finished_reduce == m.total_reduce {
			m.mu.Unlock()
			reply.TaskType = 3
		} else {
			//scan if reduce_log_pipe has 0 item
			allocated_idx := -1
			for idx, reduce_item := range m.reduce_log_pipe {
				if reduce_item == 0 {
					allocated_idx = idx
					break
				}
			}
			if allocated_idx == -1 {
				m.mu.Unlock()
				reply.TaskType = 2
			} else {
				m.reduce_log_pipe[allocated_idx] = 1
				m.mu.Unlock()
				reply.TaskType = 1
				reply.ReduceTaskId = allocated_idx
				reply.TotalMap = m.total_map
				go func(idx int) {
					time.Sleep(10 * time.Second)
					m.mu.Lock()
					if m.reduce_log_pipe[idx] == 1 {
						m.reduce_log_pipe[idx] = 0
					}
					m.mu.Unlock()
				}(allocated_idx)
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = (m.total_reduce == m.finished_reduce)

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.total_map = len(files)
	m.total_reduce = nReduce
	m.finished_map = 0
	m.finished_reduce = 0
	m.map_log_pipe = make([]int, m.total_map)
	m.reduce_log_pipe = make([]int, m.total_reduce)
	m.server()
	return &m
}
