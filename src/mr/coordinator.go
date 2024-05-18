package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files          []string
	reduceNum      int // 启动多少个 reducer
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	TaskInfoMap    map[int]*TaskInfo
	Phase          Phase // master 处于什么阶段？
	nextTaskID     int   // 主键自增来生成 TaskID
}

// 存储 Task 的元信息
type TaskInfo struct {
	state State
}

// State 任务的状态的父类型
type State int

// State 状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// Phase 对于分配任务阶段的父类型
type Phase int

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskRequestArgs, reply *Task) error {
	reply.TaskType = MapTask
	reply.FileName = c.files
	reply.ReduceNum = c.reduceNum
	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:          files,
		reduceNum:      nReduce,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		TaskInfoMap:    make(map[int]*TaskInfo, len(files)+nReduce), // 任务总数为 file + reducer
		Phase:          MapPhase,
	}

	// Your code here.
	c.makeMapTasks(files)

	c.server()
	return &c
}

// 初始化 map task
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		id := c.generateTaskID()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.reduceNum,
			FileName:  []string{file},
		}

		// 任务初始状态
		taskInfo := &TaskInfo{state: Waiting}
		// 将 task 状态保存在 master 中
		c.TaskInfoMap[task.TaskId] = taskInfo
		c.MapTaskChan <- &task
	}
}

// 返回 c.nextTaskID，然后自增
func (c *Coordinator) generateTaskID() int {
	ret := c.nextTaskID
	c.nextTaskID++
	return ret
}
