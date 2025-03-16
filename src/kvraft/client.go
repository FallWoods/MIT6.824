package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const RpcRetryInterval = time.Millisecond * 20

type Clerk struct {
	servers []*labrpc.ClientEnd // 若干个KVserver节点，有一个是Leader
	// You will have to modify this struct.
	leaderId  int   // 当前的Leader的索引
	clientId  int64 // 唯一标识客户端
	commandId uint  // 递增的序列号，唯一标识每个客户端的每条指令
}

func (ck *Clerk) GetCommandId() (SendId uint) {
	SendId = ck.commandId
	ck.commandId++
	return
}

// 随机生成一个64位整数作为的客户端标识
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// ck := new(Clerk)
	// ck.servers = servers
	// You'll have to add code here.

	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{key, ck.GetCommandId(), ck.clientId}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrLeaderOutDated {
			if !ok {
				reply.Err = ErrRpcFailed
			}
			ck.leaderId++
			ck.leaderId %= len(ck.servers)
			time.Sleep(RpcRetryInterval)
			continue
		}

		switch reply.Err {
		case ErrHandleOpTimeOut:
			time.Sleep(RpcRetryInterval)
			continue
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{key, value, op, ck.GetCommandId(), ck.clientId}

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrLeaderOutDated {
			if !ok {
				reply.Err = ErrRpcFailed
			}
			ck.leaderId++
			ck.leaderId %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrHandleOpTimeOut:
			time.Sleep(RpcRetryInterval)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
