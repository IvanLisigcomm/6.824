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
	"strconv"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 全局锁，避免多个 Worker 同时向 Master 申请任务
var mu_worker sync.Mutex

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// fmt.Println("##########")
		task := Ask_task()
		switch task.Phase {
		case "map":
			// fmt.Println("##########")
			Do_map(task, mapf)
		case "reduce":
			Do_reduce(task, reducef)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Ask_task() Task_info {
	mu_worker.Lock()
	defer mu_worker.Unlock()
	args := Task_Args{}
	reply := Task_Reply{}
	ok := call("Coordinator.Allocate_task", &args, &reply)
	if !ok {
		fmt.Println("RPC c.Allocate_task err")
	}
	// fmt.Println(reply.Task)
	return reply.Task
}

func Do_map(task Task_info, mapf func(string, string) []KeyValue) {
	filename := task.Task_name
	intermediate := []KeyValue{}
	nReduce := task.ReduceNum
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("cannot open file", filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("cannot open file", filename)
		return
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	reduce_data := [][]KeyValue{}
	for i := 0; i < nReduce; i++ {
		reduce_data = append(reduce_data, []KeyValue{})
	}
	for i := 0; i < len(intermediate); i++ {
		reduce_id := ihash(intermediate[i].Key) % nReduce
		reduce_data[reduce_id] = append(reduce_data[reduce_id], intermediate[i])
	}
	Generate_File(nReduce, reduce_data, task.Task_id)
	Modify_task_status(task.Task_id)
	// fmt.Println("!!!!!!!!!")
}

func Do_reduce(task Task_info, reducef func(string, []string) string) {
	nMap := task.MapNum
	task_id := task.Task_id
	oname := "mr-out-" + strconv.Itoa(task_id)
	ofile, _ := os.Create(oname)

	kva := []KeyValue{}
	for j := 0; j < nMap; j++ {
		filename := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(task_id)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("cannot open file: ", filename)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	Modify_task_status(task_id)
	// fmt.Println("@@@@@@@@")
}

func Generate_File(nReduce int, reduce_data [][]KeyValue, task_id int) {
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(task_id) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		encoder := json.NewEncoder(ofile)
		for _, kv := range reduce_data[i] {
			encoder.Encode(kv)
		}
		ofile.Close()
	}
}

func Modify_task_status(task_id int) {
	mu_worker.Lock()
	defer mu_worker.Unlock()
	ok := call("Coordinator.Modify_task", &task_id, nil)
	if !ok {
		fmt.Println("RPC Master.ModifyTask err......")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
