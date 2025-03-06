package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "errors"
import "os"
import "strconv"

// Add your RPC definitions here.
type MsgType int

var (
	BadMsgType = errors.New("bad message type")
	NoMoreTask = errors.New("no more task left")
)

const (
	AskForTask MsgType = iota 	// work请求任务
	MapTaskAlloc			  	// Coordinator分配Map任务
	ReduceTaskAlloc			  	// Coordinator分配Reduce任务
	MapSuccess				  	// Worker报告Map任务的执行成功
	MapFailed					// Worker报告Map任务的执行失败
	ReduceSuccess				// Worker报告Reduce任务的执行成功
	ReduceFailed				// Worker报告Reduce任务的执行失败
	Shutdown					// Coordinator告知Worker退出（所有任务执行成功)
	Wait						// Coordinator告知Worker休眠（暂时没有任务需要执行）
)

// work发送给Coordinator的消息
type MessageSend struct {
	MessageCategory MsgType
	TaskId int	// Worker回复的消息类型如MapSuccess等需要使用
}

type MessageReply struct {
	MessageCategory MsgType
	NReduce int		// MapTaskAlloc需要告诉Map Task 切分的数量
	TaskId int		// 任务Id用于选取输入文件
	MapTaskName string // MapAlloc专用: 告知输入.txt文件的名字
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
