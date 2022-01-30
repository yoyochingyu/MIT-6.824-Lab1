package mr

// RPC definitions.
import "os"
import "strconv"

type Args struct {
	ProcessId int
	Task      *Task
}

type Reply struct {
	Task *Task
	R    int
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
