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
	//"log"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//"fmt"
	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	//"honnef.co/go/tools/analysis/report"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// 当每个Raft对等体意识到连续的日志条目被提交时，
// 该对等体应该通过传递给Make（）的applyCh向同一服务器上的服务（或测试人员）发送ApplyMsg。
// 将CommandValid设置为true，表示ApplyMsg包含新提交的日志条目。
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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


type LogEntry struct {
	Command		interface{}
	Term		int
}

type RaftTimer struct {
	Timer		*time.Timer
}



func (rt *RaftTimer)ticker(rf *Raft) {
	for !rf.killed() {
		select {
		case <- rt.Timer.C:
			rf.mu.Lock()
			
			if rf.role == Leader {
				// 发送心跳
				go rf.sendHeartBeat()
				rt.Timer.Reset(50 * time.Millisecond)
			} else {
				now := time.Now().UnixMilli()

				if rf.role == Candidate || rf.timeout <= now {
					
					// 选举超时 650ms ~ 900ms
					random := rand.Int63n(250) + 650
					go requestVote(rf)
					rt.Timer.Reset(time.Duration(random) * time.Millisecond)
					rf.timeout = now + random;
				} else {
					rt.Timer.Reset(time.Duration(rf.timeout - now) * time.Millisecond)
				}
			}
			rf.mu.Unlock()
		}
	}
}

const Leader int = 1
const Candidate int = 2
const Follower int = 3

//
// A Go object implementing a single Raft peer.
// 每个服务器都有Raft结构，执行命令先通过Raft层
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role			int			// 角色

	timer			RaftTimer
	timeout			int64
	voteNum			int

	currentTerm		int
	votedFor		int

	logs			[]LogEntry
	applyCh			chan ApplyMsg

	commitIndex		int			// 已提交日志的索引
	lastApplied		int			// 最后一个apply的索引

	lastIncludedIndex	int		// 快照包含的最后一个日志索引
	lastIncludedTerm	int		// 快照包含的最后一个日志任期

	nextIndex		[]int		// 表示领导人将要发送给该追随者的下一条日志条目的索引
	matchIndex		[]int		// 标志已发送的最高日志索引

}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前term 和 是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// 获取所有日志的长度 （包括快照）
func (rf *Raft) allLength() (int) {
	return len(rf.logs) + rf.lastIncludedIndex
}

// 获取logs的长度（不包括快照）
func (rf *Raft) realLength() (int) {
	return len(rf.logs)
}

// 最后一个日志条目的索引（包括快照）
func (rf *Raft) lastIndex() (int) {
	return rf.lastIncludedIndex + len(rf.logs)
}

// 最后一个日志条目的任期（包括快照）
func (rf *Raft) lastTerm() (int) {
	lastTerm := 0
	if(len(rf.logs) > 0) {
		lastTerm = rf.logs[len(rf.logs) - 1].Term
	} else {
		lastTerm = rf.lastIncludedTerm
	}
	return lastTerm
}

// index在logs中的索引
func (rf *Raft) indexToReal(index int) (int) {
	if(rf.lastIncludedIndex == 0) {
		return index - 1
	}
	return index - rf.lastIncludedIndex - 1; 
}

// logs的索引转换为全局索引
func (rf *Raft) realToAll(index int) (int) {
	if(rf.lastIncludedIndex == 0) {
		return index + 1
	}
	return index + rf.lastIncludedIndex + 1;
}

func (rf *Raft) raftstate() ([]byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return  w.Bytes()
}

// 持久化数据
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	
	rf.persister.SaveRaftState(rf.raftstate())
}


// 读取持久化数据
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.logs)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= 0 || index <= rf.lastIncludedIndex {
		return
	}

	snaplog := make([]LogEntry, 0)
	for i := rf.indexToReal(index + 1); i < rf.realLength(); i++ {
		snaplog = append(snaplog, rf.logs[i])
	}
	rf.lastIncludedTerm = rf.logs[rf.indexToReal(index)].Term
	rf.lastIncludedIndex = index
	rf.logs = snaplog
	if index > rf.commitIndex{
		rf.commitIndex = index
	}
	if index > rf.lastApplied{
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.raftstate(), snapshot)
}


type InstallSnapshotArgs struct {
	Term					int
	LeaderId				int
	LastIncludedIndex		int
	LastIncludedTerm		int
	Data					[]byte		// 快照
}

type InstallSnapshotReply struct {
	Term		int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// 否则更新超时时间
	random := rand.Int63n(200) + 250
	rf.timer.Timer.Stop()
	rf.timer.Timer.Reset(time.Duration(random) * time.Millisecond)
	rf.timeout = time.Now().UnixMilli() + random
	
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId

	if args.LastIncludedIndex <= rf.commitIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// 需要保留日志
	if args.LastIncludedIndex < rf.lastIndex() && args.LastIncludedIndex > rf.lastIncludedIndex && args.LastIncludedTerm == rf.logs[rf.indexToReal(args.LastIncludedIndex)].Term {
		logs := make([]LogEntry, 0)
		for i := rf.indexToReal(args.LastIncludedIndex + 1); i < rf.realLength(); i++ {
			logs = append(logs, rf.logs[i])
		}
		rf.logs = logs
	} else {
		rf.logs = nil
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persister.SaveStateAndSnapshot(rf.raftstate(), args.Data)


	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm, 
		SnapshotIndex: rf.lastIncludedIndex,
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	go func ()  {
		rf.applyCh <- msg
	}()
	
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 投票请求结构 用于RPC
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int		// 当前的任期
	Id				int		// 在peer[]中的索引 即 me
	LastLogIndex	int		// 最后一个日志的槽位
	LastLogTerm		int		// 最后一个日志的term

}

// 投票请求回复 - 用于RPC
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok		bool		// 是否投票
	Term	int			// 任期 如果不等于rf.me就忽略该投票
}

// 处理投票请求
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Ok = false
		reply.Term = rf.currentTerm
		return
	}
	// 如果对方的Term小于currentTerm则拒绝
	if args.Term < rf.currentTerm {
		reply.Ok = false
		reply.Term = rf.currentTerm
		return
	}

	// 有投票可能
	if args.Term > rf.currentTerm || rf.votedFor == -1 {
		
		// 检查日志状态
		if args.LastLogTerm < rf.lastTerm() || args.LastLogTerm == rf.lastTerm() && args.LastLogIndex < rf.allLength() {
			// 候选人日志没有本地新 拒绝投票
			rf.currentTerm = args.Term
			if rf.role == Leader {
				rf.role = Follower
			}
			reply.Ok = false
			reply.Term = rf.currentTerm
		} else {
			rf.role = Follower
			rf.votedFor = args.Id
			rf.currentTerm = args.Term

			reply.Ok = true
			reply.Term = rf.currentTerm

			// 重置选举超时
			random := rand.Int63n(250) + 650
			rf.timer.Timer.Stop()
			rf.timer.Timer.Reset(time.Duration(random) * time.Millisecond)
			rf.timeout = time.Now().UnixMilli() + random;
		}
	} else {
		reply.Ok = false
		reply.Term = rf.currentTerm
	}
	rf.persist()
	
}

// 发送投票请求
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) requestVoteReplyHandle(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 过时
	if reply.Term < rf.currentTerm || rf.role != Candidate {
		return
	}
	if reply.Ok && reply.Term == rf.currentTerm {
		
		rf.voteNum++
		if rf.voteNum > len(rf.peers) / 2 {
			rf.role = Leader
			loglen := rf.allLength()
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = loglen + 1
				rf.matchIndex[i] = 0
			}
			rf.timer.Timer.Stop()
			rf.timer.Timer.Reset(50 * time.Millisecond)
			go rf.sendHeartBeat()
			//rf.persist()
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}
}


// 追加日志结构 / 心跳包	用于RPC
type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int

	Entries			[]LogEntry
	
	CommitIndex		int
}

const (
	AppendNormal	int = iota	// 追加正常
	AppendOut					// 追加过时
	Dead						// 已退出
	Commited					// 追加日志已提交
	Mismatch					// 日志不匹配
	Lack						// 日志空缺
)

// 追加日志回复	用于RPC
type AppendEntriesReply struct {
	Term		int
	OK			bool
	State		int
	NextIndex	int			// 冲突日志条目的任期号和自己存储那个任期的最早的索引
}

// 追加日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// raft已退出
	if rf.killed() {
		reply.OK = false
		reply.State = Dead
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	entrieslen := len(args.Entries)
	last := args.PrevLogIndex + entrieslen
	// term比当前还小则拒绝 或 过期
	if args.Term < rf.currentTerm || 
		args.PrevLogIndex < rf.lastIncludedIndex || 
		(entrieslen > 0 && last <= rf.lastIndex() && args.Entries[entrieslen-1].Term == rf.lastTerm()) {
		reply.OK = false
		reply.Term = rf.currentTerm
		// 已过期的Append
		reply.State = AppendOut
		return
	}
	
	// 更新超时时间
	random := rand.Int63n(200) + 250
	rf.timer.Timer.Stop()
	rf.timer.Timer.Reset(time.Duration(random) * time.Millisecond)
	rf.timeout = time.Now().UnixMilli() + random
	
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	//log.Printf("prev = %d, lastInclude = %d, len = %d", args.PrevLogIndex, rf.lastIncludedIndex, rf.realLength())
	if args.PrevLogIndex > rf.lastIndex() {
		// 日志有空缺
		reply.OK = false
		reply.State = Lack
		reply.NextIndex = rf.lastIndex() + 1
		reply.Term = rf.currentTerm
	} else if args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm {
		reply.OK = false
		reply.State = Mismatch
		reply.NextIndex = rf.lastIncludedIndex
		reply.Term = rf.currentTerm
	} else if args.PrevLogIndex > rf.lastIncludedIndex && (rf.logs[rf.indexToReal(args.PrevLogIndex)].Term != args.PrevLogTerm) {
		// 日志不匹配
		reply.OK = false
		reply.State = Mismatch
		reply.NextIndex = 1
		// 跳过不匹配的term
		clash := rf.logs[rf.indexToReal(args.PrevLogIndex)].Term
		for i := rf.indexToReal(args.PrevLogIndex); i >= 0; i-- {
			if rf.logs[i].Term != clash {
				reply.NextIndex = rf.realToAll(i) + 1
				break
			}
		}
		reply.Term = rf.currentTerm
	} else {
		// 追加日志
		if entrieslen == 0 {

		} else if args.PrevLogIndex == 0 {
			rf.logs = args.Entries
		} else if args.PrevLogIndex + entrieslen <= rf.allLength() && args.Entries[entrieslen-1].Term == rf.logs[rf.indexToReal(args.PrevLogIndex + entrieslen)].Term {
			// 该appendEntries已过时 不需要处理
		}else {
			rf.logs = append(rf.logs[:rf.indexToReal(args.PrevLogIndex) + 1], args.Entries...)
		}
		
		rf.matchIndex[rf.me] = rf.lastIndex()
		reply.OK = true
		reply.State = AppendNormal
		reply.NextIndex = rf.lastIndex() + 1
		reply.Term = rf.currentTerm
		
		if args.CommitIndex > rf.commitIndex {
			if args.CommitIndex < rf.allLength() {
				rf.commitIndex = args.CommitIndex
			} else {
				rf.commitIndex = rf.lastIndex()
			}
		}
	}
	
	rf.persist()

}

// 请求追加日志
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	
	return ok
}

// 使用Raft的服务（例如，k/v服务器）希望开始就要附加到Raft的日志的下一个命令达成一致。
// 如果此服务器不是领导者，则返回false。否则，启动协议并立即返回。
// 由于领导人可能会失败或输掉选举，因此无法保证这一命令会被提交给Raft日志。
// 即使Raft实例已经被杀死，这个函数也应该正常返回。
// 第一个返回值是命令在提交时将出现的索引。
// 第二个返回值是当前项。如果此服务器认为它是领导者，则第三个返回值为true
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	//isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return index, term, false
	}
	
	rf.logs = append(rf.logs, LogEntry{
		Command: command,
		Term: rf.currentTerm,
	})
	index = rf.lastIndex()
	term = rf.currentTerm
	rf.matchIndex[rf.me] = index
	//fmt.Println(rf.me, "start index", index, "term:", rf.currentTerm)
	rf.persist()
	go rf.sendHeartBeat()
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func requestVote(rf *Raft) {
	
	rf.mu.Lock()
	rf.currentTerm++
	rf.role = Candidate
	rf.voteNum = 1
	rf.votedFor = rf.me
	rf.persist()
	
	args := RequestVoteArgs {
		Term:			rf.currentTerm,
		Id: 			rf.me,
		LastLogIndex:	rf.lastIndex(),
		LastLogTerm: 	rf.lastTerm(),
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply {}
			if !rf.sendRequestVote(i, &args, &reply) {
				return
			}
			rf.requestVoteReplyHandle(&reply);
		} (i)
	}
}

// 发送心跳包
func (rf *Raft) sendHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		
		go func(i int) {
			rf.mu.Lock()
			if rf.killed() || rf.role != Leader {
				rf.mu.Unlock()
				return
			}
			lastindex := rf.lastIncludedIndex
			lastterm := rf.lastIncludedTerm
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = lastindex
				args.LastIncludedTerm = lastterm
				args.Data = rf.persister.ReadSnapshot()
				rf.mu.Unlock()
				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(i, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				if reply.Term != rf.currentTerm || rf.role != Leader{
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm{
					rf.role = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[i] <= lastindex {
					rf.nextIndex[i] = lastindex + 1
				}
				if rf.matchIndex[i] < lastindex {
					rf.matchIndex[i] = lastindex
				}
				rf.mu.Unlock()
				return
			}

			arg := AppendEntriesArgs{}
			arg.Term = rf.currentTerm
			arg.LeaderId = rf.me
			arg.CommitIndex = rf.commitIndex
			nextIndex:= rf.nextIndex[i]

			reply := AppendEntriesReply{}
			
			if rf.realLength() == 0 {
				arg.PrevLogIndex = 0
				arg.PrevLogTerm = 0
				arg.Entries = nil
			} else {
				if nextIndex > rf.lastIndex() {
					arg.PrevLogIndex = rf.lastIndex()
					arg.PrevLogTerm = rf.lastTerm()
					arg.Entries = nil
				} else if nextIndex <= 1 {
					arg.PrevLogIndex = 0
					arg.Entries = rf.logs
				} else if nextIndex - 1 == rf.lastIncludedIndex {
					arg.PrevLogIndex = rf.lastIncludedIndex
					arg.PrevLogTerm = rf.lastIncludedTerm
					arg.Entries = rf.logs
				} else {
					arg.PrevLogIndex = nextIndex - 1
					//log.Printf("prev = %d, real = %d, len = %d", arg.PrevLogIndex, rf.indexToReal(arg.PrevLogIndex), rf.realLength())
					arg.PrevLogTerm = rf.logs[rf.indexToReal(arg.PrevLogIndex)].Term
					arg.Entries = rf.logs[rf.indexToReal(nextIndex):]
				}
			}
			rf.mu.Unlock()
			if !rf.sendAppendEntries(i, &arg, &reply) {
				return
			}
			rf.mu.Lock()
			if rf.killed() || rf.role != Leader || reply.State == Dead {
				rf.mu.Unlock()
				return
			}
			// Leader身份过期
			if reply.Term > rf.currentTerm{
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			} else if reply.Term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			ok := reply.OK
			state := reply.State
			nextIndex = reply.NextIndex
			if !ok {
				rf.mu.Lock()
				if state == Mismatch || state == Lack {
					rf.nextIndex[i] = nextIndex
				}
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			if nextIndex - 1 > rf.matchIndex[i] {
				rf.matchIndex[i] = nextIndex - 1
			}
			if nextIndex > rf.nextIndex[i] {
				rf.nextIndex[i] = nextIndex
				// leader不能直接提交前任的日志
				if nextIndex - 1 > rf.commitIndex && rf.indexToReal(nextIndex - 1) < rf.realLength() && rf.logs[rf.indexToReal(nextIndex-1)].Term == rf.currentTerm {
						num := 0
						for _, j := range rf.matchIndex {
							if j >= nextIndex - 1 {
								num++
								if num > len(rf.peers) / 2 {
									rf.commitIndex = nextIndex - 1
									break
								}
							}
						}
				}
			}
			rf.mu.Unlock()
		} (i)
	}
}

// 如果最近没有收到选票，那么自动投票程序将开始新的选举
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.timer.ticker(rf)
	
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		
		//fmt.Println(rf.me, "commitIndex:", rf.commitIndex, "applied:", rf.lastApplied)
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		//log.Printf("lastapp = %d, commit = %d, lastInclude = %d", lastApplied, commitIndex, rf.lastIncludedIndex);
		if lastApplied < commitIndex && lastApplied >= rf.lastIncludedIndex {
			logs := make([]LogEntry, commitIndex - lastApplied)
			copy(logs, rf.logs[rf.indexToReal(lastApplied + 1) : rf.indexToReal(commitIndex + 1)])
			rf.mu.Unlock()
			i := 0
			for lastApplied < commitIndex {
				lastApplied++;
				applyMsg := ApplyMsg{
					CommandValid        : true,
					Command				: logs[i].Command,
					CommandIndex        : lastApplied,
				} 
				rf.applyCh <- applyMsg
				i++
			}
			rf.mu.Lock()
			if(commitIndex > rf.lastApplied) {
				rf.lastApplied = commitIndex
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 创建一个Raft服务器
// 所有Raft服务器（包括这一个）的端口都在对peers[]中
// applyCh是测试人员或服务期望Raft在其上发送ApplyMsg消息的通道。
// Make（）必须快速返回
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower

	rf.applyCh = applyCh
	rf.logs = make([]LogEntry, 0)
	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludedIndex = 0;
	rf.lastIncludedTerm = 0;
		
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	
	// initialize from state persisted before a crash
	// 初始化崩溃前保持的状态
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 启动ticker goroutine启动选举
	rf.timer = RaftTimer{
		Timer: time.NewTimer(time.Duration(rand.Int63n(300)) * time.Millisecond),
	}
	go rf.ticker()
	
	// 检查commitIndex的更新
	go rf.applyLoop()


	return rf
}
