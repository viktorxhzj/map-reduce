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

// Status is...
type Status int

// Master ...
type Master struct {
	// Your definitions here.
	taskQueue chan *Task
	taskLock  sync.Mutex

	/* M */
	// mapTasksCount int
	mapTasks []Task

	/* N */
	// reduceTasksCount int
	reduceTasks []Task

	shutDown bool

	workerCount int
	countLock   sync.Mutex

	inputFiles []string

	emptyTask Task
}

// Your code here -- RPC handlers for the worker to call.

// DispatchTask dispatches a task to Work.
func (m *Master) DispatchTask(request *RPCRequest, response *RPCResponse) error {

	/* If shutDown, dispatches nothing */
	if m.shutDown == true {
		response.Task = nil
		return nil
	}

	select {
	/* Successfully dispatches */
	case task := <-m.taskQueue:
		response.Task = task
		go m.waitForTask(task)
		return nil
	/* Non-Blocking */
	default:
		response.Task = &m.emptyTask
		return nil
	}
}

func (m *Master) waitForTask(t *Task) {
	timer := time.NewTimer(MaxMasterWaitTime)
	<-timer.C
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	if t.Status != TERMINATE {
		t.Status = READY
	}
}

// GetTaskReport determines whether a Task processed by the caller Worker is completed.
func (m *Master) GetTaskReport(request *RPCRequest, response *RPCResponse) error {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	t := request.Task
	status := t.Status
	index := t.Index
	theType := t.Type

	if status == SUCCEED {
		// m.countLock.Lock()
		if theType == MapTask {
			m.mapTasks[index].Status = TERMINATE
			// if m.mapTasksCount > 0 {
			// m.mapTasksCount--
			// }
		} else {
			m.reduceTasks[index].Status = TERMINATE
			// if m.reduceTasksCount > 0 {
			// m.reduceTasksCount--
			// }
		}
		// m.countLock.Unlock()
	} else {
		t.Status = READY
	}
	return nil
}

// RegisterWorker registers a Worker to the Master, and gives it an ID
func (m *Master) RegisterWorker(request *RPCRequest, response *RPCResponse) error {
	m.countLock.Lock()
	defer m.countLock.Unlock()

	response.WorkerID = m.workerCount
	m.workerCount++

	return nil
}

// UnregisterWorker unregisters a Worker
func (m *Master) UnregisterWorker(request *RPCRequest, response *RPCResponse) error {
	m.countLock.Lock()
	defer m.countLock.Unlock()

	m.workerCount--

	return nil
}

func (m *Master) detectMap() bool {

	for i := 0; i < len(m.mapTasks); i++ {
		if m.mapTasks[i].Status != TERMINATE {
			return true
		}
	}
	return false
}

func (m *Master) detectReduce() bool {

	for i := 0; i < len(m.reduceTasks); i++ {
		if m.reduceTasks[i].Status != TERMINATE {
			return true
		}
	}
	return false
}

func (m *Master) scheduleMap() {
	/* Keep scheduling Map tasks until all Map tasks are done */
	for m.detectMap() {

		/* All Map tasks are done, exit */
		// fmt.Println("Map Tasks Count: ", m.mapTasksCount)

		// if m.mapTasksCount == 0 {
		// 	break
		// }

		for i := 0; i < len(m.mapTasks); i++ {
			status := READY
			m.taskLock.Lock()
			if m.mapTasks[i].Status == READY {
				m.mapTasks[i].Status = RUNNING
			} else {
				status = TERMINATE
			}
			m.taskLock.Unlock()

			if status == READY {
				m.taskQueue <- &m.mapTasks[i]
			}
		}

		time.Sleep(MaxTaskRunTime)
	}
	// fmt.Println("Map Exit")
}

func (m *Master) scheduleReduce() {
	/* Keep scheduling Reduce tasks until all Reduce tasks are done */
	for m.detectReduce() {

		/* All Reduce tasks are done, exit */
		// fmt.Println("Reduce Tasks Count: ", m.reduceTasksCount)

		// if m.reduceTasksCount == 0 {
		// 	break
		// }

		for i := 0; i < len(m.reduceTasks); i++ {
			status := READY
			m.taskLock.Lock()
			if m.reduceTasks[i].Status == READY {
				m.reduceTasks[i].Status = RUNNING
			} else {
				status = TERMINATE
			}
			m.taskLock.Unlock()
			if status == READY {
				m.taskQueue <- &m.reduceTasks[i]
			}
		}

		time.Sleep(MaxTaskRunTime)
	}
	// fmt.Println("Reduce Exit")
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	/* 1 在RPC服务器注册RPC接收者的方法. */
	rpc.Register(m)

	/* 2 注册向RPC服务器发送的RPC消息的HTTP处理器 */
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)

	/* 3 在RPC服务器上监听消息 */
	listener, error := net.Listen("unix", sockname)
	if error != nil {
		log.Fatal("listen error:", error)
	}

	/* 4 接受监听到的HTTP请求，并调用对应方法做出响应 */
	go http.Serve(listener, nil)
}

// Done is called to ...
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// fmt.Println("Remaining Workers: ", m.workerCount)
	if m.shutDown == true {
		// if m.shutDown == true && m.reduceTasksCount == 0 {

		ret = true
	}

	return ret
}

func (m *Master) run() {
	m.scheduleMap()
	m.scheduleReduce()
	m.shutDown = true
}

// MakeMaster creates a Master Process.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init(files, nReduce)
	go m.run()

	m.server()
	return &m
}

func (m *Master) init(files []string, nReduce int) {
	m.inputFiles = files
	m.emptyTask = Task{Type: Empty}

	m.taskQueue = make(chan *Task)
	m.taskLock = sync.Mutex{}
	m.countLock = sync.Mutex{}

	// m.mapTasksCount = len(files)
	// m.reduceTasksCount = nReduce
	m.mapTasks = make([]Task, len(files))
	m.reduceTasks = make([]Task, nReduce)

	for i := 0; i < len(files); i++ {
		m.mapTasks[i] = Task{
			M:         len(files),
			N:         nReduce,
			Index:     i,
			Type:      MapTask,
			InputFile: m.inputFiles[i],
			Status:    READY,
		}
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = Task{
			M:      len(files),
			N:      nReduce,
			Index:  i,
			Type:   ReduceTask,
			Status: READY,
		}
	}
}
