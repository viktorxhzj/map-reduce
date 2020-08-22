package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Worker is...
type worker struct {
	NextTask *Task
	Mapf     func(string, string) []KeyValue
	Reducef  func(string, []string) string
	id       int
}

// KeyValue is a Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *worker) run() {
	w.reg()

	/* Loop to get a task */
	for {
		w.getTask()

		/* If no task, quit */
		if w.NextTask == nil {
			break
		}
		if w.NextTask.Type == Empty {
			time.Sleep(MaxWaitTime)
			continue
		}

		/* Else, perform task */
		switch w.NextTask.Type {
		case MapTask:
			w.doMap()
		case ReduceTask:
			w.doReduce()
		}

		w.reportTask()
	}

	/* End */
	w.unreg()
}

func (w *worker) reg() {
	request := RPCRequest{}
	response := RPCResponse{}

	call("Master.RegisterWorker", &request, &response)

	w.id = response.WorkerID
}

func (w *worker) unreg() {
	request := RPCRequest{}
	request.WorkerID = w.id

	response := RPCResponse{}

	call("Master.UnregisterWorker", &request, &response)
}

func (w *worker) getTask() {
	request := RPCRequest{}

	response := RPCResponse{}

	call("Master.DispatchTask", &request, &response)

	w.NextTask = response.Task
}

func (w *worker) reportTask() {
	request := RPCRequest{}

	response := RPCResponse{}

	request.Task = w.NextTask

	call("Master.GetTaskReport", &request, &response)
}

func (w *worker) doMap() {
	task := w.NextTask

	task.Status = RUNNING
	fileName := task.InputFile
	n := task.N
	mIndex := task.Index

	contents, err := ioutil.ReadFile(fileName)
	if err != nil {
		task.Status = FAIL
		return
	}

	kvs := w.Mapf(fileName, string(contents))

	reduces := make([][]KeyValue, n)

	for _, kv := range kvs {
		idx := ihash(kv.Key) % n
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, line := range reduces {
		IMMIFileName := getIMMIFileName(mIndex, idx)
		os.Remove(IMMIFileName)
		f, err := ioutil.TempFile("./", "tmp"+IMMIFileName)
		if err != nil {
			task.Status = FAIL
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range line {
			if err := enc.Encode(&kv); err != nil {
				task.Status = FAIL
				return
			}

		}
		if err := f.Close(); err != nil {
			task.Status = FAIL
			return
		}
		os.Rename(f.Name(), IMMIFileName)
	}
	task.Status = SUCCEED
}

func (w *worker) doReduce() {
	task := w.NextTask

	task.Status = RUNNING
	m := task.M
	nIndex := task.Index

	intermediate := []KeyValue{}

	for i := 0; i < m; i++ {
		immiFileName := getIMMIFileName(i, nIndex)
		immiFile, err := os.Open(immiFileName)
		if err != nil {
			task.Status = FAIL
			return
		}
		dec := json.NewDecoder(immiFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		immiFile.Close()
	}

	sort.Sort(byKey(intermediate))

	reduceFileName := getReduceFileName(nIndex)
	os.Remove(reduceFileName)

	tmpFile, err := ioutil.TempFile("./", "tmp"+reduceFileName)
	if err != nil {
		task.Status = FAIL
		return
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.Reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			task.Status = FAIL
			return
		}

		i = j
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), reduceFileName)

	task.Status = SUCCEED
}

// Worker will be called by main/mrworker.go
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	w := worker{
		Mapf:    mapf,
		Reducef: reducef,
	}
	w.run()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	/* 1 获得master进程的socket name */
	sockname := masterSock()

	/* 2 向HTTP RPC服务器 建立连接，得到RPC Client */
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	/* 3 RPC Client 进行远程过程调用，即RPC */
	err = client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
