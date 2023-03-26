package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task_info struct {
	Phase           string    // 任务阶段：map、reduce
	Task_id         int       // 任务ID，即在Master任务列表中的下标
	Task_name       string    // 文件名
	Task_state      string    // 任务状态：waiting、allocated、finished
	Task_start_time time.Time // 任务的开始时间，用于超时判断
	ReduceNum       int       // 记录reduce任务的数量
	MapNum          int       // 记录map任务的数量
}

type Coordinator struct {
	// Your definitions here.
	task_list  []Task_info
	phase      string
	num_reduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Allocate_task(task *Task_Args, reply *Task_Reply) error {
	task_id := c.Choose_task()
	if task_id == -1 {
		c.Task_finished()
		task_id = 0
	}
	c.task_list[task_id].Task_start_time = time.Now()
	// fmt.Println(c.task_list[task_id])
	reply.Task = c.task_list[task_id]
	return nil
}

func (c *Coordinator) Choose_task() int {
	for {
		var allFinished = true
		for i := 0; i < len(c.task_list); i++ {
			if c.task_list[i].Task_state == "waiting" {
				c.task_list[i].Task_state = "allocated"
				allFinished = false
				return i
			} else if c.task_list[i].Task_state == "allocated" {
				c.MoniterTimeout()
				allFinished = false
			}
		}
		if allFinished {
			return -1
		}
	}
}

func (c *Coordinator) Task_finished() {
	switch c.phase {
	case "map":
		c.phase = "reduce"
		c.Generate_reduce_task()
	case "reduce":
		c.phase = "done"
		for {
			for _, r := range `-\|/` {
				fmt.Printf("\r%c", r)
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

func (c *Coordinator) Generate_reduce_task() {
	taskinfo := []Task_info{}
	for i := 0; i < c.num_reduce; i++ {
		task := Task_info{"reduce", i, "", "waiting", time.Now(), c.num_reduce, len(c.task_list)}
		taskinfo = append(taskinfo, task)
	}
	c.task_list = taskinfo
}

func (c *Coordinator) MoniterTimeout() {
	for i := 0; i < len(c.task_list); i++ {
		if c.task_list[i].Task_state == "allocated" && time.Since(c.task_list[i].Task_start_time) > time.Second*10 {
			c.task_list[i].Task_state = "waiting"
		}
	}
}

func (c *Coordinator) Modify_task(task_id int, reply *Task_Reply) error {
	c.task_list[task_id].Task_state = "finished"
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.phase == "done" {
		fmt.Println("所有任务执行完")
		return true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.phase = "map"
	c.num_reduce = nReduce
	// Your code here.
	for idx, filename := range files {
		task := Task_info{"map", idx, filename, "waiting", time.Now(), nReduce, len(c.task_list)}
		c.task_list = append(c.task_list, task)
	}
	// fmt.Println(c.task_list)
	c.server()
	return &c
}
