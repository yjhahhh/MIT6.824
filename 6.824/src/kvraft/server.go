package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOP		= "Get"
	PutOP		= "Put"
	AppendOP	= "Append"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command		string
	Key			string
	Value		string
	ClientId	int64
	RequestId	int64

}



type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db		map[string]string
	waitChans	map[int]chan Op	// 每个index对应的chan
	applyIndexs	map[int64]int64		// 每个client以完成请求的最大id
	lastIncludeIndex	int	// server已执行的最大index
	persister *raft.Persister

}

func (kv *KVServer) getChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.waitChans[index]
	if !exists {
		kv.waitChans[index] = make(chan Op)
	}
	return kv.waitChans[index]
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	//log.Printf("call Get...")
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.RequestId <= kv.applyIndexs[args.ClientId] {
		reply.Err = OK
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op {
		Command: GetOP,
		Key: args.Key,
		Value: "",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	waitChan := kv.getChan(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	

	select {
	case ret := <-waitChan :
		//log.Printf("wait reply...")
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
		} else {
			kv.mu.Lock()
			value, exists := kv.db[args.Key]
			if !exists {
				reply.Err = ErrNoKey
			} else {
				//log.Printf("success...")
				reply.Err = OK
			}
			kv.mu.Unlock()
			reply.Value = value
		}
		timer.Stop()
	case <-timer.C :
		//log.Printf("timeout...")
		reply.Err = ErrWrongLeader
	}
	
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	//log.Printf("call PutAppend...")
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.RequestId <= kv.applyIndexs[args.ClientId] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op {
		Command: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	//log.Printf("start reply...")
	waitChan := kv.getChan(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	
	select {
		
	case ret := <-waitChan :
		//log.Printf("server get reply")
		if ret.Command == "Snapshot" {
			reply.Err = ErrWrongLeader
		} else if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		timer.Stop()
	case <-timer.C :
		//log.Printf("timeout...")
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func(kv *KVServer) replyLoop() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh :
			if msg.CommandValid {
				op := msg.Command.(Op)
				if !kv.isDuplicate(op.ClientId, op.RequestId) {
					kv.mu.Lock()
					kv.lastIncludeIndex = msg.CommandIndex
					if op.Command == PutOP {
						kv.db[op.Key] = op.Value
					} else if op.Command == AppendOP {
						kv.db[op.Key] += op.Value
					}
					kv.applyIndexs[op.ClientId] = op.RequestId
					
					kv.mu.Unlock()
				}
				if kv.maxraftstate != -1 {
					if kv.persister.RaftStateSize() >= kv.maxraftstate {
						kv.rf.Snapshot(kv.lastIncludeIndex, kv.kvServerSnapShot())
					}
				}
				//log.Printf("get commited...%s %d-%d, intdex : %d", op.Command, op.ClientId, op.RequestId, msg.CommandIndex)
				kv.mu.Lock()
				if _, exists := kv.waitChans[msg.CommandIndex]; exists {
					ch := kv.waitChans[msg.CommandIndex]
					kv.mu.Unlock()
					go func ()  {
						ch <- op
					} ()
				} else {
					kv.mu.Unlock()
				}
			} 
			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.lastIncludeIndex = msg.SnapshotIndex
				kv.readSnapShot(msg.Snapshot)
				for _, ch := range kv.waitChans {
					ch <- Op{}
				}
				kv.waitChans = make(map[int]chan Op)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) isDuplicate(clientId int64, requestId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	seq, exists := kv.applyIndexs[clientId]
	if !exists {
		return false
	}
	return requestId <= seq
}


func (kv *KVServer)kvServerSnapShot() []byte{
	
	w := new(bytes.Buffer)
	e :=labgob.NewEncoder(w)

	if e.Encode(kv.db) != nil||
		e.Encode(kv.applyIndexs) != nil || 
		e.Encode(kv.lastIncludeIndex) != nil {
			return nil
		}
	return w.Bytes()
}

func (kv *KVServer)readSnapShot(data []byte){
	if len(data) == 0{
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var	applyIndexs map[int64]int64
	var lastInclude int
	if d.Decode(&db) !=nil ||
		d.Decode(&applyIndexs) != nil ||
		d.Decode(&lastInclude) != nil {
			return 
		}
	kv.db = db
	kv.applyIndexs = applyIndexs
	kv.lastIncludeIndex = lastInclude
}

func (kv *KVServer)snapShotLoop() {
	if kv.maxraftstate == -1 {
		return
	}
	for !kv.killed() {
		kv.mu.Lock()
		if kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.rf.Snapshot(kv.lastIncludeIndex, kv.kvServerSnapShot())
		}
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

//
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
//
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

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.waitChans = make(map[int]chan Op)
	kv.applyIndexs = make(map[int64]int64)
	kv.lastIncludeIndex = 0
	kv.persister = persister
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.readSnapShot(snapshot)
	}
	go kv.replyLoop()
	go kv.snapShotLoop()
	return kv
}
