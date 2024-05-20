package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := CallGetTask()
		switch task.TaskType {
		case MapTask:
			doMapTask(&task, mapf)
			callDone(&task)
		case ReduceTask:
			doReduceTask(&task, reducef)
			callDone(&task)
		case WaittingTask:
			// 等待一段时间再请求
			time.Sleep(5 * time.Second)
			continue
		case ExitTask:
			time.Sleep(time.Second)
			fmt.Println("exit task")
			return
		}

	}

}

func doMapTask(task *Task, mapf func(string, string) []KeyValue) {
	var intermediate []KeyValue
	filename := task.FileName[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: %v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// 获取 kv 键值对
	intermediate = mapf(filename, string(content))

	nReduce := task.ReduceNum
	// 根据 key 选择还原任务
	kvHash := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		// use the ihash(key) to pick the reduce task for a given key
		kvHash[ihash(kv.Key)%nReduce] = append(kvHash[ihash(kv.Key)%nReduce], kv)
	}

	for i := 0; i < nReduce; i++ {
		// 临时文件
		tmpfile, err := ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		defer os.Remove(tmpfile.Name())

		fn := fmt.Sprintf("mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i))

		// 将 kv 键值对 存储在 JSON 文件中
		enc := json.NewEncoder(tmpfile)
		for _, kv := range kvHash[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode error: %v", err)
			}
		}
		tmpfile.Close()

		// 原子操作重命名
		os.Rename(tmpfile.Name(), fn)
	}
}

func doReduceTask(task *Task, reducef func(string, []string) string) {
	// 使用临时文件预防崩溃
	tmpfile, err := ioutil.TempFile("", "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	// 从 JSON 文件中读取 kv 键值对
	var intermediate []KeyValue
	for _, fileName := range task.FileName {
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	// 调用 reducef 存储到临时文件
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 处理所有相同的key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 既然返回的是 len(values) = j - i, 是否不需要 values？
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// 使用原子操作重命名
	fn := fmt.Sprintf("mr-out-%v", task.ReduceNum)
	os.Rename(tmpfile.Name(), fn)
}

// 调用 Coordinator.GetTask 获取任务
func CallGetTask() Task {

	// declare an argument structure.
	args := TaskRequestArgs{}

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatal("Coordinator.GetTask failed")
	}
	fmt.Printf("GetTask %v\n", reply)
	return reply
}

func callDone(t *Task) {
	reply := Task{}
	// 通知 task 已完成，传入参数 t
	ok := call("Coordinator.MarkFinished", &t, &reply)

	if !ok {
		fmt.Printf("%v callDone failed!\n", t.TaskId)
	}
	fmt.Printf("task %v done\n", t.TaskId)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
