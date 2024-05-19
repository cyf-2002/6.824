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

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	files          []string
	reduceNum      int // 启动多少个 reducer
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	TaskMetaHolder TaskMetaHolder
	Phase          Phase // master 处于什么阶段？
	nextTaskID     int   // 主键自增来生成 TaskID
}

// 存储 Task 的元信息
type TaskInfo struct {
	state     State
	startTime time.Time
	ptr       *Task
}

type TaskMetaHolder struct {
	TaskInfoMap map[int]*TaskInfo
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

// 分发任务（核心）
func (c *Coordinator) GetTask(args *TaskRequestArgs, reply *Task) error {
	// 上锁
	mu.Lock()
	defer mu.Unlock()

	switch c.Phase {
	case MapPhase:
		{
			if len(c.MapTaskChan) > 0 {
				*reply = *<-c.MapTaskChan
				if !c.TaskMetaHolder.updateState(reply.TaskId) {
					fmt.Printf("task %v is running", reply.TaskId)
				}
			} else {
				// map task 被分发完 但还没执行完
				reply.TaskType = WaittingTask
				if c.checkPhaseDone() {
					c.nextPhase()
				}
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChan) > 0 {
				*reply = *<-c.ReduceTaskChan
				if !c.TaskMetaHolder.updateState(reply.TaskId) {
					fmt.Printf("task %v is running", reply.TaskId)
				}
			} else {
				// reduce task 被分发完 但还没执行完
				reply.TaskType = WaittingTask
				if c.checkPhaseDone() {
					c.nextPhase()
				}
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		panic("Phase undefined")
	}

	return nil
}

func (c *Coordinator) MarkFinished(t *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	info, ok := c.TaskMetaHolder.TaskInfoMap[t.TaskId]
	if ok && info.state == Working {
		info.state = Waiting
	} else {
		fmt.Printf("%v %v is already finished", t.TaskType, t.TaskId)
	}
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
		TaskMetaHolder: TaskMetaHolder{
			TaskInfoMap: make(map[int]*TaskInfo, len(files)+nReduce), // 任务总数为 file + reducer
		},
		Phase: MapPhase,
	}

	// Your code here.
	c.makeMapTasks(files)

	c.server()
	go c.crashDetector()
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
		taskInfo := TaskInfo{
			state: Waiting,
			ptr:   &task,
		}
		// 将 task 状态保存在 master 中
		c.TaskMetaHolder.putTask(&taskInfo)
		fmt.Println("making a map task :", &task)
		c.MapTaskChan <- &task
	}
	fmt.Println("done making map tasks")
}

// 初始化 reduce task
func (c *Coordinator) makeReduceTasks() {}

// 返回 c.nextTaskID，然后自增
func (c *Coordinator) generateTaskID() int {
	ret := c.nextTaskID
	c.nextTaskID++
	return ret
}

// 可不可以直接放？一定要判断吗？
func (t *TaskMetaHolder) putTask(taskInfo *TaskInfo) bool {
	id := taskInfo.ptr.TaskId
	info, _ := t.TaskInfoMap[id]
	if info != nil {
		fmt.Printf("task %v already exists\n", id)
		return false
	}

	t.TaskInfoMap[id] = taskInfo
	return true
}

// 对任务元数据进行更新 记录开始时间
func (t *TaskMetaHolder) updateState(taskId int) bool {
	info, _ := t.TaskInfoMap[taskId]
	if info.state != Waiting {
		return false
	}
	info.state = Working
	info.startTime = time.Now()
	return true
}

// 判断该 phase 是否结束,输出当前 phase 完成情况
func (c *Coordinator) checkPhaseDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, info := range c.TaskMetaHolder.TaskInfoMap {
		if info.ptr.TaskType == MapTask {
			if info.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if info.ptr.TaskType == ReduceTask {
			if info.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	fmt.Printf("map tasks %v/%v done, reduce tasks %v/%v\n",
		mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	// 这段逻辑要特别注意!!!
	// 这么写是错误的，在 reduce 阶段这个表达式恒为 true
	//if mapDoneNum > 0 && mapUnDoneNum == 0 {
	//	return true
	//}

	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	}
	if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}

	return false
}

// 切换到下一个 phase
func (c *Coordinator) nextPhase() {
	if c.Phase == MapPhase {
		c.makeReduceTasks()
		c.Phase = ReducePhase
	} else if c.Phase == ReducePhase {
		c.Phase = AllDone
	}
}

// 检测崩溃
func (c *Coordinator) crashDetector() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.Phase == AllDone {
			mu.Unlock()
			break
		}

		// 检测是否运行超过 10s
		for _, info := range c.TaskMetaHolder.TaskInfoMap {
			if info.state == Working && time.Now().Sub(info.startTime) >= 10*time.Second {
				switch info.ptr.TaskType {
				case MapTask:
					c.MapTaskChan <- info.ptr
					info.state = Waiting
				case ReduceTask:
					c.ReduceTaskChan <- info.ptr
					info.state = Waiting
				}
				fmt.Printf("crash detected task %v\n", info.ptr.TaskId)
			}
		}

		mu.Unlock()
	}
}
