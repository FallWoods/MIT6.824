package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// server的3种类型
const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 101
	ElectTimeOutBase = 450
)

type Entry struct {
	Term int
	Cmd  interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int           // 将要提交的日志的最高索引
	lastApplied int           // 已经被应用到状态机的日志的最高索引
	nextIndex   []int         // 复制到某一个follower时, log开始的索引
	matchIndex  []int         // 已经被复制到follower的日志的最高索引
	applyCh     chan ApplyMsg // 用于在应用到状态机时传递消息
	condApply   *sync.Cond

	// 以下不是Figure 2中的field
	timer *time.Timer // 记录收到消息的时间(心跳或append)
	role  int         // 当前server的角色
	rd    *rand.Rand

	muVote    sync.Mutex // 保护投票数据voteCount
	voteCount int        // 得票计数
}

func (rf *Raft) ResetTimer() {
	rdTimeOut := GetRandomElectTimeout(rf.rd)
	rf.timer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil || d.Decode(&currentTerm) != nil {
		DPrintf("readPersist failed\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // which candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     int     // o follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // Follower中与Leader冲突的Log条目对应的Term
	XIndex  int  // Follower中，对应Term为XTerm的第一条Log条目的索引
	XLen    int  // Follower的log的长度
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf为投票者
	if args.Term < rf.currentTerm {
		// 旧的term
		// 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票: 旧的term: %v,\n\targs= %+v\n", rf.me, args.CandidateId, args.Term, args)
		return
	}

	// 代码到这里时, args.Term >= rf.currentTerm
	if args.Term > rf.currentTerm {
		// 已经是新一轮的term, 之前的投票记录作废
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	// at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 首先确保是没投过票的
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			// 2. If votedFor is null or candidateId, and candidate’s log is least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.ResetTimer()
			rf.persist()

			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
			return
		}
	} else {
		DPrintf("server %v 拒绝向 server %v投票\n", rf.me, args.CandidateId)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	newEntry := &Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, *newEntry)
	rf.persist()

	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		<-rf.timer.C
		rf.mu.Lock()
		if rf.role != Leader {
			// 超时
			go rf.Elect()
		}
		rf.ResetTimer()
		rf.mu.Unlock()
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.currentTerm++    // 发起选举时，把自己的任期加1
	rf.role = Candidate // 成为候选者
	rf.votedFor = rf.me // 投票给自己
	rf.voteCount = 1    // 当前获得了1票
	// rf.timeStamp = time.Now() // 自己给自己投票也算一种消息，应当更新时间戳, 否则下一轮投票很快又来了

	DPrintf("server %v 开始发起新一轮投票, 新一轮的term为: %v", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 收集其他节点的投票
		go rf.collectVote(i, args)
	}
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}
	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role == Follower {
			// 有另外一个投票的协程收到了更新的term而更改了自身状态为Follower
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}
		rf.role = Leader
		// 当选举出新Leader时，需要重新初始化其nextIndex和matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}
	rf.muVote.Unlock()
}

func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	// args指针作为参数被发送给了多个其他服务器，因此，必须重新复制一份args，否则，共享的args会被其中一个服务器修改
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		// 易错点: 函数调用的间隙被修改了
		return false
	}

	if reply.Term > rf.currentTerm {
		// 已经是过时的term了
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v 开始发送心跳\n", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		// if the server is dead or is not the leader, just return
		if rf.role != Leader {
			// 如果不是leader了，则终止发送心跳
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				// 如果有新的log需要发送, 则就是一个真正的AppendEntries而不是心跳
				args.Entries = rf.log[rf.nextIndex[i]:]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries\n", rf.me, i)
			} else {
				// 如果没有新的log发送, 就发送一个长度为0的切片, 表示心跳
				args.Entries = make([]Entry, 0)
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			go rf.handleAppendEntries(i, args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm (§5.1)
		// 有2种情况:
		// - 这是真正的来自旧的leader的消息
		// - 当前节点是一个孤立节点, 因为持续增加 currentTerm 进行选举, 因此真正的leader返回了更旧的term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 代码执行到这里就是 args.Term >= rf.currentTerm 的情况

	// 不是旧 leader的话需要记录访问时间
	rf.ResetTimer()

	if args.Term > rf.currentTerm {
		// 来自新leader的第一个消息
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	if len(args.Entries) == 0 {
		// 说明这是心跳函数
		DPrintf("server %v 接收到 leader &%v 的心跳\n", rf.me, args.LeaderId)
	} else {
		DPrintf("server %v 收到 leader %v 的AppendEntries: %+v \n", rf.me, args.LeaderId, args)
	}

	isConflict := false

	// 校验PrevLogIndex和PrevLogTerm，发现不合法
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) {
		// PrevLogIndex位置不存在日志项
		reply.XTerm = -1
		reply.XLen = len(rf.log) // log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 检查到AppendEntriesArgs中参数不合法:\n\t args.PrevLogIndex=%v, args.PrevLogTerm=%v, \n\tlen(self.log)=%v, self最后一个位置term为:%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// PrevLogIndex位置的日志项存在, 但term不匹配
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for rf.log[i].Term == reply.XTerm {
			i--
		}
		reply.XIndex = i + 1
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置Term不匹配, args.Term=%v, 实际的term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)

	// 一条条移除的方法
	// if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
	// 	   //发生了冲突, 移除冲突位置开始后面所有的内容
	//     DPrintf("server %v 的log与args发生冲突, 进行移除\n", rf.me)
	// 	   rf.log = rf.log[:args.PrevLogIndex+1]
	// }

	// for idx, log := range args.Entries {
	// 	ridx := rf.RealLogIdx(args.PrevLogIndex) + 1 + idx
	// 	if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
	// 		// 某位置发生了冲突, 覆盖这个位置开始的所有内容
	// 		rf.log = rf.log[:ridx]
	// 		rf.log = append(rf.log, args.Entries[idx:]...)
	// 		break
	// 	} else if ridx == len(rf.log) {
	// 		// 没有发生冲突但长度更长了, 直接拼接
	// 		rf.log = append(rf.log, args.Entries[idx:]...)
	// 		break
	// 	}
	// }

	// 实际上, 不管是否冲突, 直接移除, 因为可能出现重复的RPC
	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行apeend\n", rf.me)
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.condApply.Signal() // 唤醒检查commit的协程
	}
}

func (rf *Raft) handleAppendEntries(serverTo int, args *AppendEntriesArgs) {
	// 目前的设计, 失败只需要返回，重试会发生在SendHeartBeats的下一次循环中, 所以这里不需要死循环

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		// 要先判断term是否改变, 否则后续的更改matchIndex等是不安全的, 函数调用间隙值变了, 已经不是发起这个调用时的term了
		return
	}

	if reply.Success {
		// server回复成功
		rf.matchIndex[serverTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		// 判断为提交的日志是否可以commit
		N := len(rf.log) - 1
		for N > rf.commitIndex {
			count := 1 // Leader自己已经包含此条日志条目了
			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				// 如果至少一半的follower包含了此条日志, 则提交此条日志及其之前未提交的日志
				break
			}
			N--
		}
		rf.commitIndex = N
		rf.condApply.Signal()
		return
	}

	if reply.Term > rf.currentTerm {
		// 对方回复了更大的term, 表示此节点已经不是leader了
		// 易错点: 这里也应该进行重新选举而不是直接转化为follower, 因为这可能是来自一个孤立节点的返回值
		DPrintf("server %v 旧的leader收到了来自 server % v 的心跳函数中更新的term: %v, 发起选举\n", rf.me, serverTo, reply.Term)

		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.ResetTimer()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		// term仍然相同, 且自己还是leader, 表明对应的follower在prevLogIndex位置没有与prevLogTerm匹配的项，
		// 进行快速回退
		if reply.XTerm == -1 {
			// PrevLogIndex这个位置在Follower中不存在
			rf.nextIndex[serverTo] = reply.XLen
			return
		}

		i := rf.nextIndex[serverTo] - 1
		for i > 0 && rf.log[i].Term > reply.XTerm {
			i--
		}
		if rf.log[i].Term == reply.XTerm {
			// PrevLogIndex发生冲突时, 冲突位置上Follower的Term Leader也有
			// 则下一次该发给follower的日志条目的索引为此Term最后一条日志的下一个日志
			rf.nextIndex[serverTo] = i + 1
		} else {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term Leader没有
			rf.nextIndex[serverTo] = reply.XIndex
		}
		return
	}
}

func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
	return ok
}

// CommitChecker是一个轮询的协程，其不断检查rf.commitIndex > rf.lastApplied,
// 将rf.lastApplied递增然后发送到管道applyCh
func (rf *Raft) CommitChecker() {
	// 检查是否有新的commit
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *msg
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.role = Follower
	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))
	rf.timer = time.NewTimer(0)
	rf.ResetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CommitChecker()

	return rf
}
