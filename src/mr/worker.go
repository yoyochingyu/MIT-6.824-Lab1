package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func handleMapTask(task *Task, R int, mapf func(string, string) []KeyValue) {
	// n == len(task.InputFiles) == # of go routines(mapFunctions) [1 inputFile: 1 mapFunction/goroutine]
	n := len(task.InputFiles)
	ch := make(chan []KeyValue, n)

	for _, filename := range task.InputFiles {
		go func(filename string) {
			content := readFile(filename)
			kva := mapf(filename, content)
			ch <- kva
		}(filename)
	}

	// hash each intermediate key & group into partitions
	// 0th index: reduceTask Index, 1th index: KV pair
	var partitions [][]KeyValue
	for i := 0; i < R; i++ {
		partitions = append(partitions, []KeyValue{})
	}
	for i := 0; i < n; i++ {
		intermediate := <-ch
		for _, kv := range intermediate {
			idx := ihash(kv.Key) % R
			partitions[idx] = append(partitions[idx], kv)
		}
	}

	// save partitions to file
	for i := 0; i < R; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		f, _ := os.Create(fileName)
		enc := json.NewEncoder(f)
		for _, kv := range partitions[i] {
			enc.Encode(&kv)
		}
		task.OutputFiles = append(task.OutputFiles, fileName)
	}

	(*task).Status = Completed
	args := Args{Task: task}
	reply := Reply{}
	call("Master.CompleteTask", &args, &reply)
}

func handleReduceTask(task *Task, reducef func(string, []string) string) {
	// parse inputFiles into intermediate KV pairs
	var intermediate []KeyValue
	for _, fileName := range task.InputFiles {
		f, _ := os.Open(fileName)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, _ := os.Create(oname)

	// group intermediate KV pairs by key, execute reduceFunction
	i := 0
	var wg sync.WaitGroup
	var fileLock sync.Mutex
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		wg.Add(1)
		go func(i int, values []string) {
			defer wg.Done()
			fileLock.Lock()
			output := reducef(intermediate[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			fileLock.Unlock()
		}(i, values)
		i = j
	}

	wg.Wait()
	ofile.Close()

	task.Status = Completed
	task.OutputFiles = append(task.OutputFiles, oname)

	args := Args{Task: task}
	reply := Reply{}
	call("Master.CompleteTask", &args, &reply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	requestTask(mapf, reducef)

}

func requestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := Args{ProcessId: os.Getpid()}
	reply := Reply{}
	call("Master.AssignTask", &args, &reply)

	task := reply.Task
	R := reply.R
	start := time.Now()

	switch task.TaskType {
	case Exit:
		os.Exit(0)
	case Wait:
		fmt.Println("sleep 10 sec ... waiting for all map/reduce tasks completed")
		time.Sleep(10 * 1000 * time.Millisecond)
	case MapTask:
		handleMapTask(task, R, mapf)
		fmt.Printf("Map Task %d finished within %v\n", task.TaskId, time.Since(start))
	case ReduceTask:
		handleReduceTask(task, reducef)
		fmt.Printf("Reduce Task %d finished within %v\n", task.TaskId, time.Since(start))
	}
	requestTask(mapf, reducef)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
