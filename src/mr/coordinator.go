package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskStatus int

// Task状态
const (
	idle TaskStatus = iota		// 闲置未分配
	running						// 正在运行
	finished					// 完成
	failed						// 失败
)

// Map Task 执行状态
type MapTaskInfo struct {
	TaskId int			// Task序号
	Status TaskStatus	// 执行状态
	StartTime int64		// 开始执行的时间戳
}

// Reduce Task 执行状态
type ReduceTaskInfo struct {
	// ReduceTask的序号由其所在数组下标决定, 不进行额外存储
	Status TaskStatus	// 执行状态
	StartTime int64		// 开始执行的时间戳
}

type Coordinator struct {
	// Your definitions here.
	NReduce 	int		// the number of reduce task
	MapTasks	map[string]*MapTaskInfo	// MapTaskInfo
	muMap 			sync.Mutex		// 锁
	muReduce 		sync.Mutex		// 锁
	ReduceTasks []*ReduceTaskInfo		// ReduceTaskInfo，每个ReduceTaskInfo关联的是对应其下标的Reducetask
	MapComplete bool	// 指示Map任务是否完成
	ReduceComplete bool // 指示Reduce任务是否完成
}

// 初始化协调者用于管理任务的数据结构
func (c *Coordinator) initTask(files []string) {
	for idx, fileName := range files {
		c.MapTasks[fileName] = &MapTaskInfo {
			TaskId : idx,
			Status : idle,
		}
	}
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = &ReduceTaskInfo {
			Status : idle,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MessageCategory != AskForTask {
		return BadMsgType
	}
	// c.muMap.Lock()
	// c.muReduce.Lock()
	// defer c.muMap.Unlock()
	// defer c.muReduce.Unlock()
	c.muMap.Lock()
	if !c.MapComplete {
		// 记录已经完成的Map任务的个数
		mapTaskSuccessCount := 0
		// 选择一个任务返回给worker
		for fileName, taskinfo := range c.MapTasks {
			flag := false

			// 选择闲置或者失败的任务
			if taskinfo.Status == idle || taskinfo.Status == failed {
				flag = true
			} else if taskinfo.Status == running {
				// 如果任务正在运行，判断其是否超时, 超时则重新派发
				curTime := time.Now().Unix()
				// 超过10秒，则认定超时
				if (curTime - taskinfo.StartTime) > 10 {
					flag = true
				}
			} else {
				// 以上情况都不是，说明任务已经完成
				mapTaskSuccessCount++
			}
			if flag {
				// 将选定的任务分配给这个worker
				reply.MessageCategory = MapTaskAlloc
				reply.MapTaskName = fileName
				reply.NReduce = c.NReduce
				reply.TaskId = taskinfo.TaskId
	
				// log.Printf("coordinator: apply Map Task: taskID = %v\n", reply.TaskId)
	
				// 修改状态信息
				taskinfo.Status = running
				taskinfo.StartTime = time.Now().Unix()
				c.muMap.Unlock()
				return nil
			}
		}
		if (mapTaskSuccessCount < len(c.MapTasks)) {
			// Map任务还没有全部完成，但暂时也没有可分配的任务，则令该worker暂时休眠一段时间
			reply.MessageCategory = Wait
			c.muMap.Unlock()
			return nil
		} else {
			c.MapComplete = true
		}
	}
	c.muMap.Unlock()

	// 运行到这里说明map任务都已经完成，开始分配Reduce任务
	c.muReduce.Lock()
	if !c.ReduceComplete {
		reduceTasksSuccessCount := 0
		for idx, taskinfo := range c.ReduceTasks {
			flag := false
			if taskinfo.Status == idle || taskinfo.Status == failed {
				// 闲置或失败的任务直接分配
				flag = true
			} else if taskinfo.Status == running {
				// 对于正在执行的任务，判断是否超时，若超时，直接重新分配
				curTime := time.Now().Unix()
				if (curTime - taskinfo.StartTime) > 10 {
					flag = true
				}
			}else {
				reduceTasksSuccessCount++
			}
			if flag {
				reply.MessageCategory = ReduceTaskAlloc
				reply.NReduce = c.NReduce
				reply.TaskId = idx
				// log.Printf("coordinator: apply Reduce Task: taskID = %v\n", reply.TaskId)

				taskinfo.Status = running
				taskinfo.StartTime = time.Now().Unix()
				c.muReduce.Unlock()
				return nil
			}
		}

		if reduceTasksSuccessCount < c.NReduce {
			// reduce任务没有可以分配的, 但都还未完成
			reply.MessageCategory = Wait
			c.muReduce.Unlock()
			return nil
		} else {
			c.ReduceComplete = true
		}
	}
	c.muReduce.Unlock()
	// 运行到这里说明所有任务都已经完成
	reply.MessageCategory = Shutdown
	return nil
}

func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	// Map任务成功
	if req.MessageCategory == MapSuccess {
		c.muMap.Lock()
		for _, v := range c.MapTasks {
			if v.TaskId == req.TaskId {
				v.Status = finished
				// log.Printf("coordinator: map task%v finished\n", v.TaskId)
				break
			}
		}
		c.muMap.Unlock()
	// Reduce任务成功
	} else if req.MessageCategory == ReduceSuccess {
		c.muReduce.Lock()
		c.ReduceTasks[req.TaskId].Status = finished
		c.muReduce.Unlock()
		// log.Printf("coordinator: reduce task%v finished\n", req.TaskId)
	} else if req.MessageCategory == MapFailed {
		c.muMap.Lock()
		for _, v := range c.MapTasks {
			if v.TaskId == req.TaskId {
				v.Status = failed
				// log.Printf("coordinator: map task%v failed\n", v.TaskId)
				break
			}
		}
		c.muMap.Unlock()
	} else if req.MessageCategory == ReduceFailed {
		c.muReduce.Lock()
		c.ReduceTasks[req.TaskId].Status = finished
		c.muReduce.Unlock()
		// log.Printf("coordinator: reduce task%v failed\n", req.TaskId)
	}
	return nil
}



//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")

	// 创建一个UNIX-domain socket，这是一个文件系统的路径
	sockname := coordinatorSock()
	// 如果之前已经存在，则删除之
	if err := os.Remove(sockname); err != nil && !os.IsNotExist(err) {
        log.Fatal("Error removing existing socket:", err)
        return
    }
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

	// Your code here.
	// 先确认mapTask完成
	c.muMap.Lock()
	for _, taskinfo := range c.MapTasks {
		if taskinfo.Status != finished {
			c.muMap.Unlock()
			return false
		}
	}
	c.muMap.Unlock()
	// 再确认Reduce Task 完成
	c.muReduce.Lock()
	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.Status != finished {
			c.muReduce.Unlock()
			return false
		}
	}
	c.muReduce.Unlock()
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce : nReduce,
		MapTasks : make(map[string]*MapTaskInfo),
		ReduceTasks : make([]*ReduceTaskInfo, nReduce),
		MapComplete : false,
		ReduceComplete : false,
	}


	// Your code here.
	// 由于每一个文件名就是一个map task ,需要初始化任务状态
	c.initTask(files)

	c.server()
	return &c
}
