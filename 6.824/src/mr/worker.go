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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// GetTask()
	// process()
	// Report()
	for {
		// 获取任务
		workreply := CallGetTask()
		// 执行任务
		res := false
		if workreply != nil {
			if workreply.WorkTask.TaskType == MapTask {
				res = mapHandle(workreply, mapf)
			} else if workreply.WorkTask.TaskType == ReduceTask {
				res = reduceHandle(workreply, reducef)
			} else if workreply.WorkTask.TaskType == Finished{
				break
			} else {
				time.Sleep(2)
				continue
			}
		}
		// 报告任务
		reportrely := CallReport(workreply.WorkTask.TaskType, workreply.WorkTask.FT.No, res)
		// 处理报告回复
		if reportrely.Reply != Continue {
			break
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallGetTask() *WorkReply {
	args := WorkArgs {}
	reply := WorkReply {}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return nil
}

func taskHandle(reply *WorkReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	
	task := reply.WorkTask
	if task.TaskType == MapTask {
		return mapHandle(reply, mapf)
	} else if task.TaskType == ReduceTask {
		return reduceHandle(reply, reducef)
	}
	return true
}
func mapHandle(reply *WorkReply, mapf func(string, string) []KeyValue) bool {
	// 输出中间文件mr-X-Y，X对应Map任务Id，Y对应的Reduce任务Id
	// 以Json格式
	// 可以使用ihash(key)函数为给定的键选择 reduce 任务

	task := reply.WorkTask.FT
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Printf("cannot open %v", task.FileName)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", task.FileName)
		return false
	}
	file.Close()
	intermediate := []KeyValue {}
	ret := mapf(task.FileName, string(content))
	intermediate = append(intermediate, ret...)
	// 把中间结果写入文件
	filenum := reply.NReduce
	files := make([]*os.File, filenum)
	for i := 0; i < filenum; i++ {
		oname := "mr-"
		oname = oname + strconv.Itoa(task.No) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Printf("cannot create %v", oname)
			return false
		}
		files[i] = ofile
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % filenum
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	for _, file := range files {
		file.Close()
	}
	return true
}

func reduceHandle(reply *WorkReply, reducef func(string, []string) string) bool {

	no := reply.WorkTask.FT.No
	filenum := reply.NMap
	intermediate := []KeyValue {}
	// 读取对应的 mr_*_Y 文件
	for i := 0; i < filenum; i++ {
		oname := "mr"
		oname = oname + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(no)
		file, err := os.Open(oname)
		if err != nil {
			log.Printf("cannot open %v", oname)
			file.Close()
			return false
		}
		dec := json.NewDecoder(file)
		kv := KeyValue {}
		for{
			if err := dec.Decode(&kv); err!=nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 给键值对排序
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(no)
	ofile, err := os.Create(oname)
	defer ofile.Close()
	if err != nil {
		log.Printf("cannot create %v", oname)
		return false
	}
	for i := 0; i < len(intermediate); i++ {
		j := i + 1
		values := []string {}
		values = append(values, intermediate[i].Value)
		for  j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j - 1
	}
	return true
}

func CallReport(tasktype int, no int, state bool) *ReportReply {
	args := ReportArgs{
		TS: TaskState{
			TaskType: tasktype,
			No: no,
			State: state,
		},
	}
	reply := ReportReply {}
	ok := call("Coordinator.Report", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return nil

}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	// 与rpc服务器进行连接
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
