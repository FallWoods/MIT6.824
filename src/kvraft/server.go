package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

const HandleOpTimeOut = time.Millisecond * 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OpGet int = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    int
	Key       string
	Value     string
	CommandId uint
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	waitCh     map[int]*chan result // 映射 startIndex->ch
	historyMap map[int64]*result    // 映射 client->*result
	maxMapLen  int
	db         map[string]string

	persister   *raft.Persister
	lastApplied int // 当前服务器中状态机所执行的最后一条指令的索引（在Raft中日志的索引）
}

type result struct {
	LastId  uint // 产生这条结果的指令的序号
	Err     Err
	Value   string
	ResTerm int // 这条指令被提交时的任期
}

// 生成快照
func (kv *KVServer) GenSnapShot() []byte {
	// 调用时必须持有锁mu
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.historyMap)

	severState := w.Bytes()
	return severState
}

// 加载快照
func (kv *KVServer) LoadSnapShot(snapShot []byte) {
	// 调用时必须持有锁mu
	if len(snapShot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	tmpDB := make(map[string]string)
	tmpHistory := make(map[int64]*result)
	if d.Decode(&tmpDB) != nil || d.Decode(&tmpHistory) != nil {
		DPrintf("server %v LoadSnapShot 加载快照失败\n", kv.me)
	} else {
		kv.db = tmpDB
		kv.historyMap = tmpHistory
		DPrintf("server %v LoadSnapShot 加载快照成功\n", kv.me)
	}
}

func (kv *KVServer) LogInfoReceive(opArgs *Op, logType int) {
	// logType:
	// 	0: 新的请求
	// 	1: 重复的请求
	// 	2: 旧的请求
	needPanic := false
	if logType == 0 {
	} else if logType == 1 {
	} else {
		needPanic = true
	}

	if needPanic {
		panic("没有记录更早的请求的结果")
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	opArgs := &Op{
		OpType:    OpGet,
		ClientId:  args.ClinetId,
		CommandId: args.CommandId,
		Key:       args.Key,
	}

	res := kv.HandleOp(opArgs)
	reply.Err, reply.Value = res.Err, res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opArgs := &Op{
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Key:       args.Key,
		Value:     args.Value,
	}

	if args.Op == "Put" {
		opArgs.OpType = OpPut
	} else {
		opArgs.OpType = OpAppend
	}

	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
}

func (kv *KVServer) HandleOp(opArgs *Op) (res result) {
	// 提交一个操作到Raft层，然后等待ApplyHandler执行指令后，返回结果
	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		return result{Err: ErrWrongLeader, Value: ""}
	}
	kv.mu.Lock()
	newCh := make(chan result)
	kv.waitCh[startIndex] = &newCh
	DPrintf("leader %v client %v commandId %v 的请求: 新建管道: %p\n", kv.me, opArgs.ClientId, opArgs.CommandId, &newCh)

	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	// 等待消息到达或超时
	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		DPrintf("server %v client %v command %v: 超时", kv.me, opArgs.ClientId, opArgs.CommandId)
		return
	case msg, success := <-newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			return
		} else if !success {
			// 通道已经关闭, 有另一个协程收到了消息 或 通道被更新的RPC覆盖
			// TODO: 是否需要判断消息到达时自己已经不是leader了?
			DPrintf("server %v identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v, args.Key=%+v", kv.me, opArgs.ClientId, opArgs.CommandId, opArgs.OpType, opArgs.Key)
			res.Err = ErrChanClose
			return
		} else {
			// term与一开始不匹配, 说明这个Leader可能过期了
			res.Err = ErrLeaderOutDated
			res.Value = ""
			return
		}
	}
}

func (kv *KVServer) DBExecute(op *Op) (res result) {
	// 调用该函数需要持有锁
	res.LastId = op.CommandId
	switch op.OpType {
	case OpGet:
		val, exist := kv.db[op.Key]
		if exist {
			res.Value = val
			return
		} else {
			res.Err = ErrNoKey
			res.Value = ""
			return
		}
	case OpPut:
		kv.db[op.Key] = op.Value
		return
	case OpAppend:
		val, exist := kv.db[op.Key]
		if exist {
			kv.db[op.Key] = val + op.Value
			return
		} else {
			kv.db[op.Key] = op.Value
			return
		}
	}
	return
}

// 执行从Raft层返回的已提交的指令
func (kv *KVServer) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh

		if log.CommandValid {
			// 如果传上来的是指令
			op := log.Command.(Op)
			kv.mu.Lock()

			// 如果在follower一侧, 可能这个log包含在快照中, 直接跳过
			if log.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			kv.lastApplied = log.CommandIndex

			// 需要判断这个log是否需要被再次应用
			var res result

			needApply := false
			if lastRes, exist := kv.historyMap[op.ClientId]; exist {
				if lastRes.LastId == op.CommandId {
					// clientId和commandId均相等，说明历史记录存在 直接套用历史记录
					res = *lastRes
				} else if lastRes.LastId < op.CommandId {
					needApply = true
				}
			} else {
				// 历史记录不存在
				needApply = true
			}

			if needApply {
				// 执行
				res = kv.DBExecute(&op)
				res.ResTerm = log.SnapshotTerm

				// 更新历史记录
				kv.historyMap[op.ClientId] = &res
			}

			// Leader还需要额外通知handler处理clerk回复
			ch, exist := kv.waitCh[log.CommandIndex]
			if exist {
				kv.mu.Unlock()
				// 执行一条指令后，通过channel把结果发送给HandlerOp，进行后续处理
				// 发送信息
				func() {
					defer func() {
						if recover() != nil {
							DPrintf("leader %v ApplyHandler 发现 client %v Seq %v 的管道不存在, 应该是超时被关闭了", kv.me, op.ClientId, op.CommandId)
						}
					}()
					res.ResTerm = log.SnapshotTerm
					// 执行一条指令后，通过channel把结果发送给HandlerOp，进行后续处理
					*ch <- res
				}()
				kv.mu.Lock()
			}

			// 每收到一个log就检测是否需要生成快照
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate/100*95 {
				// 当达到95%容量时需要生成快照
				snapShot := kv.GenSnapShot()
				kv.rf.Snapshot(log.CommandIndex, snapShot)
			}

			kv.mu.Unlock()
		} else if log.SnapshotValid {
			// 如果传上来的是快照，只需读取快照状态，并覆盖当前状态
			kv.mu.Lock()
			if log.SnapshotIndex >= kv.lastApplied {
				kv.LoadSnapShot(log.Snapshot)
				kv.lastApplied = log.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.historyMap = make(map[int64]*result)
	kv.db = make(map[string]string)
	kv.waitCh = make(map[int]*chan result)

	// 检查启动时是否有快照
	kv.mu.Lock()
	kv.LoadSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	go kv.ApplyHandler()

	return kv
}
