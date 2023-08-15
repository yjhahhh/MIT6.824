package shardkv

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	ClientId 	int64
	RequestId 	int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister	*raft.Persister
	kvStore		map[int]*Shard		// gid -> map[key]value
	clientRequestId map[int64]int64
	waitChans	map[int]chan CommandRequest	

	dead		int32
	shardctClerk	*shardctrler.Clerk
	currentConfig		shardctrler.Config		// 当前config
	lastConfig	shardctrler.Config		// 上一个config
	lastApplied	int
	gid2Leader	map[int]int
	gcShard		map[int]map[string]string
	pullingShard	map[int]map[string]string
}

type Shard struct {
	KV			map[string]string
	Status		int
}

const (
    // The group serves and owns the shard.
    Serving int = iota
    // The group serves the shard, but does not own the shard yet.
    Pulling
    // The group does not serve and own the partition.
    Invalid
    // The group owns but does not serve the shard.
    Erasing
)

type Command struct {
	Op   CommandType
	Data interface{}
}

// get / append / put
type CommandRequest struct {
	Key   string
	Value string
	Op    string
	ClientId 	int64
	RequestId 	int64
	Err			Err
}

// 更新配置
type UpdateConfig struct {
	Config		shardctrler.Config

}

// 新增shard
type PullShard struct {
	ShardId		int
	Kv			map[string]string
	RequestIds	map[int64]int64
}

// 移除shard
type GCShard struct {
	ShardId		int
}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)


func (kv *ShardKV) checkShard(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) getChan(index int) chan CommandRequest {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.waitChans[index]
	if !exists {
		kv.waitChans[index] = make(chan CommandRequest)
	}
	return kv.waitChans[index]
}

func (kv *ShardKV) chanExists(index int) bool {
	_, exists := kv.waitChans[index]
	return exists
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	targetShard := key2shard(args.Key)
	if !kv.checkShard(targetShard) {
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Lock()
	if args.RequestId <= kv.clientRequestId[args.ClientId] {
		reply.Err = OK
		reply.Value = kv.kvStore[targetShard].KV[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command {
		Op: Operation,
		Data: CommandRequest {
			Key: args.Key,
			Op: "Get",
			ClientId: args.ClientId,
			RequestId: args.RequestId,
		},
	}
	index, _, isLeader := kv.rf.Start(command)
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
	//log.Printf("receive Get %d %d", args.ClientId, args.RequestId)
	timer := time.NewTicker(100 * time.Millisecond)

	select {
	case ret := <-waitChan :
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = ret.Err
			reply.Value = ret.Value
		}
		timer.Stop()
	case <-timer.C :
		//log.Printf("timeout...")
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	targetShard := key2shard(args.Key)
	
	if !kv.checkShard(targetShard) {
		reply.Err = ErrWrongGroup
		return
	}
	
	kv.mu.Lock()
	if args.RequestId <= kv.clientRequestId[args.ClientId] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command {
		Op: Operation,
		Data: CommandRequest {
			Key: args.Key,
			Value: args.Value,
			Op: args.Op,
			ClientId: args.ClientId,
			RequestId: args.RequestId,
		},
	}
	index, _, isLeader := kv.rf.Start(command)
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
	timer := time.NewTicker(150 * time.Millisecond)

	select {
	case ret := <-waitChan :
		timer.Stop()
		if ret.ClientId != args.ClientId || ret.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = ret.Err
			
		}
		//log.Printf("target shard : %d current group : %d", targetShard, kv.gid)
		
	case <-timer.C :
		reply.Err = ErrWrongLeader
	}
}


func (kv *ShardKV) applyLoop() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh :
			if msg.CommandValid {
				command := msg.Command.(Command)
				kv.mu.Lock()
				kv.lastApplied = msg.CommandIndex
				kv.mu.Unlock()
				if command.Op == Operation {
					op := command.Data.(CommandRequest)
					shard := key2shard(op.Key)
					if !kv.checkShard(shard) {
						op.Err = ErrWrongGroup
						if kv.chanExists(msg.CommandIndex) {
							kv.getChan(msg.CommandIndex) <- op
						}
						continue
					}
					if kv.isDuplicate(shard, op.ClientId, op.RequestId) {
						continue
					}
					if op.Op == "Get" {
						go kv.getHandle(&op, msg.CommandIndex)
					} else if op.Op == "Append" {
						go kv.appendHandle(&op, msg.CommandIndex)
					} else if op.Op == "Put" {
						go kv.putHandle(&op, msg.CommandIndex)
					}
				} else if command.Op == Configuration {
					config := command.Data.(UpdateConfig)
					go kv.updateConfigHandle(&config)
				} else if command.Op == InsertShards {
					pullData := command.Data.(PullShard)
					go kv.pullShardHandle(&pullData)
				} else if command.Op == DeleteShards {
					gcShard := command.Data.(GCShard)
					go kv.gcShardHandle(&gcShard)
				}
			} 
			if msg.SnapshotValid {
				/*
				kv.mu.Lock()
				kv.lastApplied = msg.SnapshotIndex
				kv.readSnapShot(msg.Snapshot)
				for _, ch := range kv.waitChans {
					ch <- Op{}
				}
				kv.waitChans = make(map[int]chan Op)
				kv.mu.Unlock()
				*/
			}
			
		}
	}
}

func (kv *ShardKV) getHandle(op *CommandRequest, index int) {
	// 判断当前分片是否可读
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	response := CommandRequest{
		ClientId: op.ClientId,
		RequestId: op.RequestId,
		Op: "Get",
		Key: op.Key,
	}
	if kv.currentConfig.Shards[shardId] != kv.gid {
		response.Err = ErrWrongGroup
	} else {
		// 正常提服务
		if kv.kvStore[shardId].Status == Serving {
			value, exists := kv.kvStore[shardId].KV[op.Key]
			if !exists {
				response.Err = ErrNoKey
				log.Printf("Get nokey : %s", op.Key)
			} else {
				response.Err = OK
				response.Value = value
				//log.Printf("Get : %s", value)
			}
			kv.clientRequestId[op.ClientId] = op.RequestId
		} else {
			log.Print("Get shard no serving")
			response.Err = ErrWrongGroup
		}
	}
	
	if kv.chanExists(index) {
		kv.waitChans[index] <- response
	}
}

func (kv *ShardKV) appendHandle(op *CommandRequest, index int) {
	// 判断当前分片是否可读
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	response := CommandRequest{
		ClientId: op.ClientId,
		RequestId: op.RequestId,
		Op: "Append",
		Key: op.Key,
	}
	if kv.currentConfig.Shards[shardId] != kv.gid {
		response.Err = ErrWrongGroup
	} else {
		// 正常提服务
		if kv.kvStore[shardId].Status == Serving {
			kv.kvStore[shardId].KV[op.Key] += op.Value
			//log.Printf("Append : %s value = %s", op.Key, op.Value)
			response.Err = OK
			kv.clientRequestId[op.ClientId] = op.RequestId
		} else {
			log.Print("Append shard no serving")
			response.Err = ErrWrongGroup
		}
	}
	if kv.chanExists(index) {
		kv.waitChans[index] <- response
	}
}

func (kv *ShardKV) putHandle(op *CommandRequest, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	
	response := CommandRequest{
		ClientId: op.ClientId,
		RequestId: op.RequestId,
		Op: "Put",
		Key: op.Key,
	}
	if kv.currentConfig.Shards[shardId] != kv.gid {
		response.Err = ErrWrongGroup
	} else {
		// 正常提服务
		if kv.kvStore[shardId].Status == Serving {
			kv.kvStore[shardId].KV[op.Key] = op.Value
			//log.Printf("Put : %s value = %s", op.Key, op.Value)
			response.Err = OK
			kv.clientRequestId[op.ClientId] = op.RequestId
		} else {
			log.Print("Put shard no serving")
			response.Err = ErrWrongGroup
		}
	}
	if kv.chanExists(index) {
		kv.waitChans[index] <- response
	}
}

func (kv *ShardKV) isDuplicate(shard int, clientId int64, requestId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	seq, ex := kv.clientRequestId[clientId]
	if !ex {
		return false
	}
	return requestId <= seq
}

func (kv *ShardKV) updateConfigHandle(config *UpdateConfig) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Config.Num <= kv.currentConfig.Num {
		return
	}
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = config.Config
	for shardId, gid := range config.Config.Shards {
		if gid == kv.gid && kv.lastConfig.Shards[shardId] != gid {
			kv.kvStore[shardId] = new(Shard)
			if config.Config.Num > 1 {
				kv.kvStore[shardId].Status = Pulling
				kv.pullingShard[shardId] = make(map[string]string)
			} else {
				kv.kvStore[shardId].Status = Serving
				kv.kvStore[shardId].KV = make(map[string]string)
			}
		}
	}
	for shardId := range kv.kvStore {
		if config.Config.Shards[shardId] != kv.gid {
			kv.kvStore[shardId].Status = Erasing
			kv.gcShard[shardId] = kv.kvStore[shardId].KV
			kv.kvStore[shardId].KV = nil
		}
	}
}

func (kv *ShardKV) pullShardHandle(data *PullShard) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.kvStore[data.ShardId].Status == Pulling {
		kv.kvStore[data.ShardId].KV = make(map[string]string)
		for key, value := range data.Kv {
			kv.kvStore[data.ShardId].KV[key] = value
		}
		for clientId, requestId := range data.RequestIds {
			if kv.clientRequestId[clientId] < requestId {
				kv.clientRequestId[clientId] = requestId
			}
		}
		kv.kvStore[data.ShardId].Status = Serving
		delete(kv.pullingShard, data.ShardId)
	}
}

func (kv *ShardKV) gcShardHandle(data *GCShard) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.kvStore[data.ShardId].Status == Erasing {
		delete(kv.kvStore, data.ShardId)
		delete(kv.gcShard, data.ShardId)
	}
}

func (kv *ShardKV) checkConfigLoop() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		// 分片都处于服务状态
		if isLeader && kv.checkShardStatus() {
			// 查询最新日志
			ret := kv.shardctClerk.Query(kv.currentConfig.Num + 1)
			kv.mu.Lock()
			if ret.Num == kv.currentConfig.Num + 1 {
				command := Command {
					Op: Configuration,
					Data: UpdateConfig {
					Config: ret,
					},
				}
				kv.rf.Start(command)
			}
			kv.mu.Unlock()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// 拉取待拉取的shard
func (kv *ShardKV) updateShardsLoop() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		shardIds := make([]int, 0)
		kv.mu.Lock()
		for shardId := range kv.pullingShard {
			shardIds = append(shardIds, shardId)
		}
		kv.mu.Unlock()
		var wg sync.WaitGroup
		for _, shardId := range shardIds {
			kv.mu.Lock()
			gid := kv.lastConfig.Shards[shardId]
			kv.mu.Unlock()
			wg.Add(1)
			go func(shardId int) {
				defer wg.Done()
				args := PullArgs {
					Num: kv.currentConfig.Num,
					Shard: shardId,
				}
				if servers, ok := kv.lastConfig.Groups[gid]; ok {
					srv := kv.make_end(servers[kv.gid2Leader[gid]])
					var reply PullReply
					ok := srv.Call("ShardKV.Pull", &args, &reply)
					if ok && (reply.Err == OK) {
						requestIds := make(map[int64]int64, 0)
						kv.mu.Lock()
						for clientId, requestId := range kv.clientRequestId {
							requestIds[clientId] = requestId
						}
						kv.mu.Unlock()
						command := Command {
							Op: InsertShards,
							Data: PullShard{
								ShardId: shardId,
								Kv: reply.KV,
								RequestIds: requestIds,
							},
						}
						_, _, is := kv.rf.Start(command)
						
						if !is {
							return
						}
					}
					if ok && (reply.Err == ErrWrongLeader) {
						kv.mu.Lock()
						kv.gid2Leader[gid] = (kv.gid2Leader[gid] + 1) % len(servers)
						kv.mu.Unlock()
					}
				}
			} (shardId)
		}
		wg.Wait()
		time.Sleep(150 * time.Millisecond)
	}
}

type PullArgs struct {
	Num		int
	Shard	int
}

type PullReply struct {
	Err		Err
	KV		map[string]string
}

func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 判断配置号是否满足
	if kv.currentConfig.Num != args.Num {
		reply.Err = ErrConfig
		return
	}
	// 检查分片是否满足
	if kv.kvStore[args.Shard].Status != Erasing {
		reply.Err = "ErrShardStatus"
		return
	}
	reply.Err = OK
	reply.KV = make(map[string]string)
	for key, value := range kv.gcShard[args.Shard] {
		reply.KV[key] = value
	}
}

func (kv *ShardKV) checkShardStatus() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shard := range kv.kvStore {
		if shard.Status != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) gcShardsLoop() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		shardIds := make([]int, 0)
		kv.mu.Lock()
		for shardId := range kv.gcShard {
			shardIds = append(shardIds, shardId)
		}
		kv.mu.Unlock()
		var wg sync.WaitGroup
		
		for shardId := range shardIds {
			wg.Add(1)
			go func(shardId int) {
				defer wg.Done()
				gid := kv.currentConfig.Shards[shardId]
				args := CompleteArgs {
					Num: kv.currentConfig.Num,
					Shard: shardId,
				}
				if servers, ok := kv.lastConfig.Groups[gid]; ok {
					srv := kv.make_end(servers[kv.gid2Leader[gid]])
					var reply CompleteReply
					ok := srv.Call("ShardKV.CheckComplete", &args, &reply)
					if ok && (reply.Err == OK) {
						command := Command {
							Op: DeleteShards,
							Data: GCShard{
								ShardId: shardId,
							},
						}
						_, _, is := kv.rf.Start(command)
						if !is {
							return
						}
					}
					if ok && (reply.Err == ErrWrongLeader) {
						kv.gid2Leader[gid] = (kv.gid2Leader[gid] + 1) % len(servers)
					}
				}
			}(shardId)
		}
		wg.Wait()
		time.Sleep(250 * time.Millisecond)
	}
}

type CompleteArgs struct{
	Num		int
	Shard	int
}

type CompleteReply struct {
	Err		Err
}

func (kv *ShardKV) CheckComplete(args *CompleteArgs, reply *CompleteReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 判断配置号是否满足
	if kv.currentConfig.Num != args.Num {
		reply.Err = ErrConfig
		return
	}
	// 检查分片是否满足
	if kv.kvStore[args.Shard].Status == Serving {
		reply.Err = OK
	} else {
		reply.Err = ErrConfig
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) init() {
	kv.lastConfig = shardctrler.Config{}
	kv.currentConfig = kv.shardctClerk.Query(-1)
	for shardId, gid := range kv.currentConfig.Shards {
		if gid == kv.gid {
			kv.kvStore[shardId] = new(Shard)
			kv.kvStore[shardId].Status = Serving
			kv.kvStore[shardId].KV = make(map[string]string)
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(CommandRequest{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShard{})
	labgob.Register(GCShard{})
	labgob.Register(UpdateConfig{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.kvStore = make(map[int]*Shard)
	kv.clientRequestId = make(map[int64]int64)
	kv.waitChans = make(map[int]chan CommandRequest)
	kv.dead = 0
	kv.shardctClerk = shardctrler.MakeClerk(ctrlers)
	
	kv.lastApplied = 0
	kv.gid2Leader = make(map[int]int)

	kv.gcShard = make(map[int]map[string]string)
	kv.pullingShard = make(map[int]map[string]string)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.init()
	go kv.applyLoop()
	go kv.checkConfigLoop()
	go kv.updateShardsLoop()
	go kv.gcShardsLoop()

	return kv
}
