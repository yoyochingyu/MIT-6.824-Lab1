package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	M                    int           // # of mapTasks (== pg-*.txt counts)
	R                    int           // # of reduceTasks
	idleMap              []*Task       // queue, all idle mapTasks
	idleReduce           []*Task       // queue, all idle reduceTasks
	mapTasks             map[int]*Task // all mapTasks
	reduceTasks          map[int]*Task // all reduceTasks
	completedMapTasks    int
	completedReduceTasks int
	finished             bool
	mu                   sync.Mutex
}

type Status string

const (
	Idle       Status = "IDLE"
	InProgress Status = "IN_PROGRESS"
	Completed  Status = "COMPLETED"
)

type TaskType string

const (
	MapTask    TaskType = "MAP_TASK"
	ReduceTask TaskType = "REDUCE_TASK"
	Exit       TaskType = "EXIT"
	Wait       TaskType = "WAIT"
)

type Task struct {
	TaskType    TaskType
	TaskId      int
	Status      Status
	ProcessId   int
	InputFiles  []string
	OutputFiles []string
}

// monitorTask monitors task and see if it is finished in given time or if it is timeout.
// if it is, increment completedMapTasks or completedReduceTasks.
// if not, push the task into idleMap or idleReduce again.
// monitorTask judges the status of a task based on the execution of CompleteTask.
func (m *Master) monitorTask(taskId int, taskType TaskType) {
	time.Sleep(10 * 1000 * time.Millisecond)

	m.mu.Lock()
	if taskType == MapTask {
		task := m.mapTasks[taskId]
		if task.Status != Completed {
			task.Status = Idle
			task.ProcessId = -1
			m.idleMap = append(m.idleMap, task)
		} else {
			// isCompleted, append each partition to reduceTask
			for i := 0; i < m.R; i++ {
				file := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
				m.reduceTasks[i].InputFiles = append(m.reduceTasks[i].InputFiles, file)
			}
			m.completedMapTasks++
		}
	} else {
		task := m.reduceTasks[taskId]
		if task.Status != Completed {
			task.Status = Idle
			task.ProcessId = -1
			m.idleReduce = append(m.idleReduce, task)
		} else {
			// isCompleted
			m.completedReduceTasks++
			if m.completedReduceTasks == m.R {
				m.finished = true
			}
		}
	}
	m.mu.Unlock()
}

// AssignTask is an RPC handler used to assign task to worker.
func (m *Master) AssignTask(args *Args, reply *Reply) error {
	var task *Task
	processId := args.ProcessId

	m.mu.Lock()
	if len(m.idleMap) != 0 {
		// assign mapTask to the worker

		// pop a task from the queue and shrink
		task = m.idleMap[0]
		m.idleMap = m.idleMap[1:]

		task.Status = InProgress
		task.ProcessId = processId
		reply.Task = task
		reply.R = m.R

		go m.monitorTask(task.TaskId, MapTask)
	} else if m.completedMapTasks != m.M {
		// we wait until all mapTasks complete before start assigning reduceTask
		task = &Task{TaskType: Wait}
		reply.Task = task
	} else if len(m.idleReduce) != 0 {
		// assign reduceTask to worker

		// pop a task from the queue and shrink
		task = m.idleReduce[0]
		m.idleReduce = m.idleReduce[1:]

		task.Status = InProgress
		task.ProcessId = processId
		reply.Task = task
		reply.R = m.R

		go m.monitorTask(task.TaskId, ReduceTask)
	} else if m.completedReduceTasks != m.R {
		// we wait until all reduceTask finish before closing Master
		task = &Task{TaskType: Wait}
		reply.Task = task
	} else {
		task = &Task{TaskType: Exit}
		reply.Task = task
	}

	m.mu.Unlock()
	return nil
}

// CompleteTask is an RPC handler to mark task as completed.
func (m *Master) CompleteTask(args *Args, reply *Reply) error {
	m.mu.Lock()
	task := args.Task
	if task.TaskType == MapTask {
		m.mapTasks[task.TaskId] = task
	} else {
		m.reduceTasks[task.TaskId] = task
	}
	m.mu.Unlock()
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

// main/mrmaster.go calls Done() periodically to find out if the entire job has finished.
func (m *Master) Done() bool {
	return m.finished
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		M:           len(files),
		R:           nReduce,
		idleMap:     []*Task{},
		idleReduce:  []*Task{},
		mapTasks:    make(map[int]*Task),
		reduceTasks: make(map[int]*Task),
	}

	// create mapTasks & place in idleMap & mapTasks
	for i, f := range files {
		task := Task{
			TaskType:   MapTask,
			TaskId:     i,
			Status:     Idle,
			ProcessId:  -1,
			InputFiles: []string{f},
		}
		m.idleMap = append(m.idleMap, &task)
		m.mapTasks[i] = &task
	}

	// create reduceTasks & place in idleReduce & reduceTasks
	for i := 0; i < m.R; i++ {
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    i,
			Status:    Idle,
			ProcessId: -1,
		}
		m.idleReduce = append(m.idleReduce, &task)
		m.reduceTasks[i] = &task
	}

	m.server()
	return &m
}
