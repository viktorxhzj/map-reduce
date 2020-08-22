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

// Add your RPC definitions here.

// ReportResponse is
type ReportResponse struct {
}

// RPCRequest is...
type RPCRequest struct {
	Task     *Task
	WorkerID int
}

// RPCResponse is...
type RPCResponse struct {
	Task     *Task
	WorkerID int
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
