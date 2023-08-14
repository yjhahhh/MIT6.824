package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "container/list"
import "sync"
import "time"

/*
1. nReduce对应的Reduce数及输出的文件数，也要作为MakeCoordinator()方法的参数；
2. Reduce任务的输出文件的命名为mr-out-X，这个X就是来自nReduce；
3. mr-out-X的输出有个格式要求，参照main/mrsequential.go，"%v %v" 格式；
4. Map输出的中间值要放到当前目录的文件中，Reduce任务从这些文件来读取；
5. 当Coordinator.go的Done()方法返回true，MapReduce的任务就完成了；
6. 当一个任务完成，对应的worker就应该终止，这个终止的标志可以来自于call()方法，若它去给Master发送请求，
   得到终止的回应，那么对应的worker进程就可以结束了。
*/

type FileTask struct {
	FileName string
	No int
}

type FileWait struct {
	FT FileTask
	TimeStamp int64
}

type ReduceWait struct {
	No int
	TimeStamp int64
}

type Coordinator struct {
	// Your definitions here.
	// 可分发的文件列表
	nextList list.List
	// 已分发待完成的文件列表
	dispatchedList list.List
	// 已完成的文件列表
	finishedList list.List
	// 输入文件数
	nMap int
	// 要输出文件数
	nReduce int
	// 待分配reduce任务列表
	reduceList list.List
	// 已分配reduce任务列表
	waitList list.List
	// 互斥锁
	lock sync.Mutex
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.nMap = len(files)
	c.nReduce = nReduce
	for i, filename := range files {
		c.nextList.PushBack(FileTask {
			FileName: filename,
			No: i,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceList.PushBack(i)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *WorkArgs, reply *WorkReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// 如果还有map任务
	if c.nextList.Len() != 0 {
		task := c.dispatch()
		reply.WorkTask.TaskType =  MapTask
		reply.WorkTask.FT = task
		reply.NReduce = c.nReduce
	} else if c.dispatchedList.Len() != 0 {
		// 查看是否有已分配的Map任务超时
		task, timeout := c.checkTimeout()
		if timeout {
			reply.WorkTask.TaskType = MapTask
			reply.WorkTask.FT = task
			reply.NReduce = c.nReduce
		} else {
			reply.WorkTask.TaskType = NOTask
		}
	} else if c.reduceList.Len() == 0 {
		if c.waitList.Len() == 0 {
			// reduce任务已完成
			reply.WorkTask.TaskType = Finished
		} else {
			// 是否有超时的reduce任务
			now := time.Now().Unix()
			for i := c.waitList.Front(); i != nil; i = i.Next() {
				if now - i.Value.(ReduceWait).TimeStamp > 10 {
					no := i.Value.(ReduceWait).No
					i.Value = ReduceWait{
						No: no,
						TimeStamp: now,
					}
					reply.WorkTask.TaskType = ReduceTask
					reply.WorkTask.FT.No = no
					reply.NMap = c.nMap
					break
				}
			}
		}
	} else {
		// 分配Reduce任务
		reply.WorkTask.TaskType = ReduceTask
		reply.WorkTask.FT.No = c.reduceList.Front().Value.((int))
		reply.NMap = c.nMap
		c.reduceList.Remove(c.reduceList.Front())
		c.waitList.PushBack(ReduceWait{
			No: reply.WorkTask.FT.No,
			TimeStamp: time.Now().Unix(),
		})
	}
	return nil
}

func (c *Coordinator) dispatch() FileTask {
	next := c.nextList.Front()
	c.nextList.Remove(next)
	task := next.Value.(FileTask)
	c.dispatchedList.PushBack(FileWait {
		FT: task,
		TimeStamp: time.Now().Unix(),
	})
	return task
}

func (c *Coordinator) checkTimeout() (FileTask, bool) {
	res := false
	var ret FileTask
	now := time.Now().Unix()
	for i := c.dispatchedList.Front(); i != nil; i = i.Next() {
		task := i.Value.(FileWait)
		// 任务超过10秒没完成
		if(now - task.TimeStamp > 10) {
			res = true
			ret = task.FT
			i.Value = FileWait {
				FT: ret,
				TimeStamp: now,
			}
			break
		}
	}
	return ret, res
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.TS.TaskType == MapTask {
		var task FileTask
		for i := c.dispatchedList.Front(); i != nil; i = i.Next() {
				if i.Value.(FileWait).FT.No == args.TS.No {
					task = i.Value.(FileWait).FT
					c.dispatchedList.Remove(i)
					break
				}
			}
		// 任务成功
		if args.TS.State {
			// 移入完成队列
			c.finishedList.PushBack(task)
			reply.Reply = Continue
		} else {
			// 移入待分配队列
			c.nextList.PushFront(task)
			reply.Reply = Continue
		}
	} else {
		for i := c.waitList.Front(); i != nil; i = i.Next() {
			if(i.Value.(ReduceWait).No == args.TS.No) {
				c.waitList.Remove(i)
				break
			}
		}
		// reduce任务报告
		if args.TS.State {
			if c.reduceList.Len() == 0 && c.waitList.Len() == 0 {
				reply.Reply = Finish
			} else {
				reply.Reply = Continue
			}
		} else {
			// reduce任务失败
			c.reduceList.PushFront(args.TS.No)
			reply.Reply = Finish
		}
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// 注册rpc服务，默认名字是类型名
	rpc.Register(c)
	// http作为通信协议
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
// 定期检查整个作业是否已完成
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.reduceList.Len() == 0 && c.waitList.Len() == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// files 是需要处理的文件
// nReduce对应输出文件数
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.init(files, nReduce)

	c.server()
	return &c
}
