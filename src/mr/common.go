package mr

import (
	"strconv"
	"time"
)

// TaskType is
type TaskType int

// TaskStatus is
type TaskStatus int

// Task is
type Task struct {
	M         int
	N         int
	Index     int
	Type      TaskType
	InputFile string
	Status    TaskStatus
}

// Some consts
const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	Empty      TaskType = 2

	READY     TaskStatus = 0
	RUNNING   TaskStatus = 1
	SUCCEED   TaskStatus = 2
	FAIL      TaskStatus = 3
	TERMINATE TaskStatus = 4

	MaxTaskRunTime    = time.Millisecond * 500
	MaxWaitTime       = time.Millisecond * 500
	MaxMasterWaitTime = time.Second * 2
)

func getIMMIFileName(m int, n int) string {
	return "mr-" + strconv.Itoa(m) + "-" + strconv.Itoa(n)
}

func getReduceFileName(n int) string {
	return "mr-out-" + strconv.Itoa(n)
}
