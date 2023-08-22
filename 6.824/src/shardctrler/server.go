package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead	int32
	lastIncludeIndex int

	configs []Config // indexed by config num

	waitChans	map[int]chan Op
	applyIndexs	map[int64]int64
	gid2ShardIds	map[int][]int
	freeList	[]int
}


type Op struct {
	// Your data here.
	Command		string
	GIDs		[]int
	Shard		int
	GID			int	
	Num			int
	ClientId	int64
	RequestId	int64
	Servers map[int][]string
}

func (sc *ShardCtrler) getChan(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, exists := sc.waitChans[index]
	if !exists {
		sc.waitChans[index] = make(chan Op)
	}
	return sc.waitChans[index]
}

func (sc *ShardCtrler) redistribution() {
	var gidList []int
	for gid := range sc.gid2ShardIds {
		gidList = append(gidList, gid)
	}
	if len(gidList) == 0 {
		return
	}
	sort.Ints(gidList)
	avg := NShards / len(gidList)
	if avg == 0 {
		avg = 1
	}
	for _, gid := range gidList {
		if len(sc.gid2ShardIds[gid]) > avg {
			for i := avg; i < len(sc.gid2ShardIds[gid]); i++ {
				sc.freeList = append(sc.freeList, sc.gid2ShardIds[gid][i])
			}
			sc.gid2ShardIds[gid] = sc.gid2ShardIds[gid][:avg]
		}
	}
	sort.Ints(sc.freeList)
	for _, gid := range gidList {
		if len(sc.freeList) == 0 {
			break
		}
		i := 0
		for len(sc.gid2ShardIds[gid]) < avg {
			sc.gid2ShardIds[gid] = append(sc.gid2ShardIds[gid], sc.freeList[i])
			//log.Printf("gid %d append %d", gid, sc.freeList[i])
			i++
		}
		sc.freeList = sc.freeList[i:]
	}
	for _, gid := range gidList {
		if len(sc.freeList) > 0 {
			//log.Printf("gid %d append %d", gid, sc.freeList[0])
			sc.gid2ShardIds[gid] = append(sc.gid2ShardIds[gid], sc.freeList[0])
			sc.freeList = sc.freeList[1:]
			
		} else {
			break
		}
	}
	
}

func (sc * ShardCtrler) showGid2Shards() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for gid, shards := range sc.gid2ShardIds {
		for _, shard := range shards {
			log.Printf("shard %d : gid %d", shard, gid)
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.killed() {
		sc.mu.Unlock()
		reply.Err = Dead
		return
	}
	if args.RequestId <= sc.applyIndexs[args.ClientId] {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	op := Op {
		Command: "Join",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
		Servers: args.Servers,
	}
	index, _, isLeader := sc.Raft().Start(op);
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	waitChan := sc.getChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChans, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case ret := <-waitChan :
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
		//sc.showGid2Shards()
		timer.Stop()
	case <-timer.C :
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) joinHandle(op *Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	groupsTemp := make(map[int][]string)
	for gid, servers := range sc.configs[len(sc.configs) - 1].Groups {
		groupsTemp[gid] = servers
	}
	// 加入新shard
	for gid, servers := range op.Servers {
		groupsTemp[gid] = servers
		sc.gid2ShardIds[gid] = make([]int, 0)
	}
	// 重分配
	sc.redistribution()
	var shardsTemp [NShards]int
	for gid, shards := range sc.gid2ShardIds {
		for _, shard := range shards {
			shardsTemp[shard] = gid
		}
	}
	//log.Printf("server %d join config num : %d", sc.me, len(sc.configs))
	sc.configs = append(sc.configs, Config{
		Num: len(sc.configs),
		Shards: shardsTemp,
		Groups: groupsTemp,
	})
	
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.killed() {
		sc.mu.Unlock()
		reply.Err = Dead
		return
	}
	if args.RequestId <= sc.applyIndexs[args.ClientId] {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	op := Op {
		Command: "Leave",
		GIDs: args.GIDs,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := sc.Raft().Start(op);
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	waitChan := sc.getChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChans, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)

	select {
	case ret := <-waitChan :
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
		//sc.showGid2Shards()
		timer.Stop()
	case <-timer.C :
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) leaveHandle(op *Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	groupsTemp := make(map[int][]string)
	for gid, servers := range sc.configs[len(sc.configs) - 1].Groups {
		groupsTemp[gid] = servers
	}
	// 删除shard
	for _, gid := range op.GIDs {
		sc.freeList = append(sc.freeList, sc.gid2ShardIds[gid]...)
		delete(groupsTemp, gid)
		delete(sc.gid2ShardIds, gid)
	}
	
	// 重分配
	sc.redistribution()
	var shardsTemp [NShards]int
	//log.Printf("gid len %d", len(sc.gid2ShardIds))
	//log.Printf("freeliost len %d", len(sc.freeList))
	for gid, shards := range sc.gid2ShardIds {
		for _, shard := range shards {
			shardsTemp[shard] = gid
			//log.Printf("shard %d : gid %d", shard, gid)
		}
	}
	//log.Printf("leave config num : %d", len(sc.configs))
	sc.configs = append(sc.configs, Config{
		Num: len(sc.configs),
		Shards: shardsTemp,
		Groups: groupsTemp,
	})
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.killed() {
		sc.mu.Unlock()
		reply.Err = Dead
		return
	}
	if args.RequestId <= sc.applyIndexs[args.ClientId] {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	op := Op {
		Command: "Move",
		Shard: args.Shard,
		GID: args.GID,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := sc.Raft().Start(op);
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	waitChan := sc.getChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChans, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)

	select {
	case ret := <-waitChan :
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
		timer.Stop()
	case <-timer.C :
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) moveHandle(op *Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	conf := sc.configs[len(sc.configs) - 1]
	for i, v := range sc.gid2ShardIds[conf.Shards[op.Shard]] {
		if v == op.Shard {
			sc.gid2ShardIds[conf.Shards[op.Shard]] = append(sc.gid2ShardIds[conf.Shards[op.Shard]][:i], sc.gid2ShardIds[conf.Shards[op.Shard]][i + 1:]...)
			sc.gid2ShardIds[op.GID] = append(sc.gid2ShardIds[op.GID], op.Shard)
			break
		}
	}
	conf.Num = len(sc.configs)
	conf.Shards[op.Shard] = op.GID
	//log.Printf("move config num : %d", len(sc.configs))
	sc.configs = append(sc.configs, conf)
	
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.killed() {
		sc.mu.Unlock()
		reply.Err = Dead
		return
	}
	if args.RequestId <= sc.applyIndexs[args.ClientId] {
		reply.Err = OK
		reply.WrongLeader = false
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs) - 1]
			//log.Printf(":server %d reply config %d", sc.me, len(sc.configs) - 1)
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	_, leader := sc.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	op := Op {
		Command: "Query",
		Num: args.Num,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := sc.Raft().Start(op);
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	waitChan := sc.getChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChans, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case ret := <-waitChan :
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			//log.Printf("server %d Query %d", sc.me, op.Num)
			if args.Num < 0 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs) - 1]
				//log.Printf(":server %d reply config %d", sc.me, len(sc.configs) - 1)
			} else {
				reply.Config = sc.configs[args.Num]
			}
			
		}
		timer.Stop()
	case <-timer.C :
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}



//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) replyLoop() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh :
			if msg.CommandValid {
				op := msg.Command.(Op)
				if !sc.isDuplicate(op.ClientId, op.RequestId) {
					sc.mu.Lock()
					sc.lastIncludeIndex = msg.CommandIndex
					sc.applyIndexs[op.ClientId] = op.RequestId
					sc.mu.Unlock()
					//log.Printf("server %d %s", sc.me, op.Command)
					if op.Command == "Join" {
						sc.joinHandle(&op)
					} else if op.Command == "Leave" {
						sc.leaveHandle(&op)
					} else if op.Command == "Move" {
						sc.moveHandle(&op)
					}
				}
				sc.mu.Lock()
				if _, exists := sc.waitChans[msg.CommandIndex]; exists {
					ch := sc.waitChans[msg.CommandIndex]
					sc.mu.Unlock()
					go func ()  {
						ch <- op
					} ()
				} else {
					sc.mu.Unlock()
				}
			}
		}
	}
}

func (sc *ShardCtrler) isDuplicate(clientId int64, requestId int64) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	seq, exists := sc.applyIndexs[clientId]
	if !exists {
		return false
	}
	return requestId <= seq
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.dead = 0
	sc.lastIncludeIndex = 0
	// Your code here.
	sc.waitChans = make(map[int]chan Op)
	sc.gid2ShardIds = make(map[int][]int)
	sc.applyIndexs = make(map[int64]int64)
	sc.freeList = make([]int, 0)
	for i := 0; i < NShards; i++ {
		sc.freeList = append(sc.freeList, i)
	}

	go sc.replyLoop()

	return sc
}
