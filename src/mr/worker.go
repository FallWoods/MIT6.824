package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"encoding/json"
	"io"
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

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

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
		// 循环请求任务
		reply := CallForTask()

		switch reply.MessageCategory {
		case MapTaskAlloc:
			// 分配了Map任务
			err :=HandleMapTask(reply, mapf)
			if err == nil {
				_ = CallForReportStatus(MapSuccess, reply.TaskId)
            } else {
				// log.Println("Worker: Map Task failed, error: ", err)
				_ = CallForReportStatus(MapFailed, reply.TaskId)
			}
		case ReduceTaskAlloc:
			// 分配了Reduce任务
			err := HandleReduceTask(reply, reducef)
			if err == nil {
				_ = CallForReportStatus(ReduceSuccess, reply.TaskId)
			} else {
				// log.Println("Worker: Map Task failed, error", err)
				_ = CallForReportStatus(ReduceFailed, reply.TaskId)
			}
		case Wait :
			// 没有任务可分配，但任务还没执行完，不能终止，只能等待一会
			time.Sleep(time.Second * 5)
		case Shutdown :
			// 所有任务执行完毕，终止
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}
}

func HandleMapTask(reply *MessageReply, mapf func (string, string) []KeyValue) error {
	file, err := os.Open(reply.MapTaskName)
	if err != nil {
		return err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	// 进行map
	kvarr := mapf(reply.MapTaskName, string(content))
	sort.Sort(ByKey(kvarr))

	// // 中间输出文件名的前缀
	// oname_prefix := "mr-out-" + strconv.Itoa(reply.TaskId) + "-"
	// key_vgroup := make(map[string][]string)
	// for _, kv := range kvarr {
	// 	key_vgroup[kv.Key] = append(key_vgroup[kv.Key], kv.Value)
	// }
	// // 先清理可能存在的垃圾，先前执行的任务可能失败，存在着未完成的文件
	// // TODO: 原子重命名的方法
	// _ = DelFileByMapId(reply.TaskId,"./")

	// for key, values := range key_vgroup {
	// 	// 根据键计算中间键值对的输出文件
	// 	redId := ihash(key)
	// 	oname := oname_prefix + strconv.Itoa(redId % reply.NReduce)
	// 	var ofile *os.File
	// 	if _, err = os.Stat(oname); os.IsNotExist(err) {
	// 		// 文件不存在，则创建之
	// 		ofile, _ = os.Create(oname)
	// 	} else {
	// 		// 文件存在，则只是打开它
	// 		ofile, _ = os.OpenFile(oname, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	// 	}
	// 	enc := json.NewEncoder(ofile)
	// 	for _, value := range values {
	// 		err := enc.Encode(&KeyValue{Key: key, Value :value})
	// 		if err != nil {
	// 			ofile.Close()
	// 			return err
	// 		}
	// 	}
	// 	ofile.Close()
	// }
	// 原子重命名方法
	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for _, kv := range kvarr {
		redId := ihash(kv.Key) % reply.NReduce
		if encoders[redId] == nil {
			tempFile, err := os.CreateTemp("./",fmt.Sprintf("mr-map-tmp-%d",redId))
			if err != nil {
				return err
			}
			defer tempFile.Close()
			tempFiles[redId] = tempFile
			encoders[redId] = json.NewEncoder(tempFile)
		}
		err := encoders[redId].Encode(&kv)
		if err != nil {
			return err
		}
	}
	for i, file :=  range tempFiles {
		if file != nil {
			fileName := file.Name()
			newName := fmt.Sprintf("mr-out-%d-%d", reply.TaskId, i)
			if err := os.Rename(fileName, newName); err != nil {
				return err
			}
		}
	}
	return nil
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string ) string) error {
	reduceNum := reply.TaskId
	key_vgroup := make(map[string][]string)

	fileList, err := ReadSpecificFile(reduceNum,"./")
	if err != nil {
		return err
	}
	// 合并每个MapTask所产生的1个中间文件
	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			key_vgroup[kv.Key] = append(key_vgroup[kv.Key], kv.Value)
		}
		file.Close()
	}

	// 获取所有的键并排序
	var keys []string
	for k := range key_vgroup {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oname := "mr-out-" +strconv.Itoa(reply.TaskId)

	ofile, err := os.Create(oname)

	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, key_vgroup[key])
		_, err = fmt.Fprintf(ofile,"%v %v\n",key,output)
		if err != nil {
			return err
		}
	}

	DelFileByReduceId(reply.TaskId,"./")
	return nil
}

// 向coordinator报告执行情况
func CallForReportStatus(messageType MsgType, taskId int) error {
	// 报告Task执行情况
	// declare an argument structure.
	args := MessageSend {
		MessageCategory : messageType,
		TaskId : taskId,
	}
	// 通过rpc调用Coordinator.NoticeResult()，不需要返回值，因此，第二个指针为nil
	err := call("Coordinator.NoticeResult",&args,nil)
	return err
}

// worker向coordinator请求一个任务
func CallForTask() *MessageReply {
	// declare an argument structure.
	args := MessageSend {
		MessageCategory : AskForTask,
	}
	// declare a reply structure.
	reply := MessageReply{}

	// send the RPC request, wait for the reply.
	err := call("Coordinator.AskForTask", &args, &reply)

	if err== nil {
		// fmt.Printf("TaskName %v, NReduce %v, taskID %v\n", reply.MapTaskName, reply.NReduce, reply.TaskId)
        return &reply
    } else {
		// log.Println(err.Error())
		return nil
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// 获得服务器的UNIX-domain socket，这是一个文件系统的路径
	sockname := coordinatorSock()
	// 拨号，DialHTTPPATH根据制定的网络地址和路径连接到一个HTTP RPC服务，返回与服务器连接的客户端。
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
		os.Exit(-1)
	}
	defer c.Close()
	// Call调用指定名字的服务器的方法，等待它完成，然后返回成功或失败的error状态
	err = c.Call(rpcname, args, reply)
	return err
}
