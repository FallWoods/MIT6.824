package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	commandId uint
}

func (ck *Clerk) GetCommandId() (SendId uint) {
	SendId = ck.commandId
	ck.commandId++
	return
}

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
			ck.leaderId++
			ck.leaderId %= len(ck.servers)
			continue
		}

		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrHandleOpTimeOut:
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
			ck.leaderId++
			ck.leaderId %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrHandleOpTimeOut:
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
