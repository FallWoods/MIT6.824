package kvraft

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrHandleOpTimeOut = "ErrHandleOpTimeOut"
	ErrLeaderOutDated  = "ErrLeaderOutDated"
	ErrRpcFailed       = "ErrRpcFailed"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId uint  // 指令序号
	ClientId  int64 // 唯一标识客户端
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandId uint
	ClinetId  int64
}

type GetReply struct {
	Err   Err
	Value string
}
