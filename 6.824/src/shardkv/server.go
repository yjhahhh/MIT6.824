package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

/*
	1、只有当前所有分片都处于Serving状态才接受新的config 即pull完毕和gc完毕
	2、仅当被拉取的shard处于Rrasing 并处于同一config num
	3、当shard处于Serving时同意checkComplete，或当前config num大于对端

*/

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
	//kvStore		map[int]*Shard		// shardId -> map[key]value
	store		map[int]map[string]string
	status		map[int]int
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

func (kv *ShardKV) doSnapshot() {
	
	if kv.maxraftstate != -1 {
		kv.persister.SaveStateAndSnapshot(kv.rf.Raftstate(), kv.kvServerSnapShot())
		//kv.rf.Snapshot(kv.lastApplied, kv.kvServerSnapShot())
	}
	
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
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
		reply.Value = kv.store[targetShard][args.Key]
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
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	kv.mu.Unlock()
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
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
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
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	kv.mu.Unlock()
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
				if kv.lastApplied >= msg.CommandIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				kv.mu.Unlock()
				if command.Op == Operation {
					op := command.Data.(CommandRequest)
					shard := key2shard(op.Key)
					if !kv.checkShard(shard) {
						op.Err = ErrWrongGroup
						kv.mu.Lock()
						if kv.chanExists(msg.CommandIndex) {
							kv.waitChans[msg.CommandIndex] <- op
						}
						kv.mu.Unlock()
						continue
					}
					if kv.isDuplicate(op.ClientId, op.RequestId) {
						continue
					}
					if op.Op == "Get" {
						kv.getHandle(&op, msg.CommandIndex)
					} else if op.Op == "Append" {
						kv.appendHandle(&op, msg.CommandIndex)
					} else if op.Op == "Put" {
						kv.putHandle(&op, msg.CommandIndex)
					}
				} else if command.Op == Configuration {
					config := command.Data.(UpdateConfig)
					//log.Printf("%d apply config %d", kv.gid, config.Config.Num)
					kv.updateConfigHandle(&config)
				} else if command.Op == InsertShards {
					pullData := command.Data.(PullShard)
					kv.pullShardHandle(&pullData)
				} else if command.Op == DeleteShards {
					gcShard := command.Data.(GCShard)
					kv.gcShardHandle(&gcShard)
				}
			} 
			if msg.SnapshotValid {
				
				kv.mu.Lock()
				if msg.SnapshotIndex >= kv.lastApplied {
					kv.readSnapShot(msg.Snapshot)
					kv.waitChans = make(map[int]chan CommandRequest)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
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
		if kv.status[shardId] == Serving {
			value, exists := kv.store[shardId][op.Key]
			if !exists {
				response.Err = ErrNoKey
				//log.Printf("Get nokey : %s", op.Key)
			} else {
				response.Err = OK
				response.Value = value
				//log.Printf("Get : %s", value)
			}
			kv.clientRequestId[op.ClientId] = op.RequestId
		} else {
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
		if kv.status[shardId] == Serving {
			kv.store[shardId][op.Key] += op.Value
			//log.Printf("%d server %d Append %s value  %s in config %d", kv.gid, kv.me, op.Key, op.Value, kv.currentConfig.Num)
			response.Err = OK
			kv.clientRequestId[op.ClientId] = op.RequestId
			kv.doSnapshot()
		} else {
			
			//log.Print("Append shard no serving")
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
		if kv.status[shardId] == Serving {
			kv.store[shardId][op.Key] = op.Value
			//log.Printf("Put : %s value = %s", op.Key, op.Value)
			response.Err = OK
			kv.clientRequestId[op.ClientId] = op.RequestId
			kv.doSnapshot()
		} else {
			response.Err = ErrWrongGroup
		}
	}
	if kv.chanExists(index) {
		kv.waitChans[index] <- response
	}
}

func (kv *ShardKV) isDuplicate(clientId int64, requestId int64) bool {
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
	//log.Printf("%d update config %d", kv.gid, kv.currentConfig.Num)
	for shardId, gid := range config.Config.Shards {
		if gid == kv.gid && kv.lastConfig.Shards[shardId] != gid {
			kv.store[shardId] = make(map[string]string)
			if config.Config.Num > 1 {
				kv.status[shardId] = Pulling
				kv.pullingShard[shardId] = make(map[string]string)
				//log.Printf("%d change Pulling %d", gid, shardId)
			} else {
				kv.status[shardId] = Serving
				kv.lastConfig = shardctrler.Config{}
			}
		}
	}
	for shardId := range kv.store {
		if config.Config.Shards[shardId] != kv.gid {
			kv.status[shardId] = Erasing
			kv.gcShard[shardId] = kv.store[shardId]
			//log.Printf("%d Erasing %d", kv.gid, shardId)
			kv.store[shardId] = nil
		}
	}
	//log.Printf("%d gcShard len %d", kv.gid, len(kv.gcShard))
}

func (kv *ShardKV) pullShardHandle(data *PullShard) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.status[data.ShardId]; !exists {
		//log.Printf("%d shardId %d not exists in kv.status", kv.gid, data.ShardId)
		return
	}
	if kv.status[data.ShardId] == Pulling {
		for key, value := range data.Kv {
			kv.store[data.ShardId][key] = value
		}
		for clientId, requestId := range data.RequestIds {
			if kv.clientRequestId[clientId] < requestId {
				kv.clientRequestId[clientId] = requestId
			}
		}
		kv.status[data.ShardId] = Serving
		//log.Printf("%d serving %d", kv.gid, data.ShardId)
		delete(kv.pullingShard, data.ShardId)
	} else {
		//log.Printf("%d shardId %d not Pulling", kv.gid, data.ShardId)
	}
}

func (kv *ShardKV) gcShardHandle(data *GCShard) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.status[data.ShardId]; !exists {
		return
	}
	if kv.status[data.ShardId] == Erasing {
		//log.Printf("%d delete shard %d OK", kv.gid, data.ShardId)
		delete(kv.store, data.ShardId)
		delete(kv.gcShard, data.ShardId)
		delete(kv.status, data.ShardId)
	}
}

func (kv *ShardKV) checkConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()
		// 分片都处于服务状态
		if isLeader && kv.checkShardStatus() {
			// 查询最新日志
			ret := kv.shardctClerk.Query(kv.currentConfig.Num + 1)
			//log.Printf("%d get new config %d but not %d", kv.gid, ret.Num, kv.currentConfig.Num + 1)
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
		time.Sleep(100 * time.Millisecond)
	}
}

// 拉取待拉取的shard
func (kv *ShardKV) updateShardsLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()
		if !isLeader {
			//log.Printf("%d updateShardsLoop %d is not leader...", kv.gid, kv.me)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		//log.Printf("%d server %d begin loop...", kv.gid, kv.me)
		shardIds := make([]int, 0)
		kv.mu.Lock()
		for shardId := range kv.pullingShard {
			shardIds = append(shardIds, shardId)
		}
		kv.mu.Unlock()
		var wg sync.WaitGroup
		//log.Printf("%d updateShards Looping...", kv.gid)
		for _, shardId := range shardIds {
			kv.mu.Lock()
			gid := kv.lastConfig.Shards[shardId]
			kv.mu.Unlock()
			wg.Add(1)
			//log.Printf("%d pulling %d", kv.gid, shardId)
			go func(shardId int) {
				defer wg.Done()
				kv.mu.Lock()
				args := PullArgs {
					Num: kv.currentConfig.Num,
					Shard: shardId,
					Gid: gid,
				}
				servers, ok := kv.lastConfig.Groups[gid]
				if len(servers) == 0 {
					//log.Printf("in config %d group %d is empty!", kv.currentConfig.Num, gid)
				}
				kv.mu.Unlock()
				if ok {
					for _, server := range servers {
						srv := kv.make_end(server)
						var reply PullReply
						//log.Printf("%d server %d Call Pull", kv.gid, kv.me)
						ok := srv.Call("ShardKV.Pull", &args, &reply)
						//log.Printf("%d server %d Call Pull finish!", kv.gid, kv.me)
						if !ok {
							//log.Printf("%d Pull RPC fail!", kv.gid)
						}
						if ok && (reply.Err == OK) {
							command := Command {
								Op: InsertShards,
								Data: PullShard{
									ShardId: shardId,
									Kv: reply.KV,
									RequestIds: reply.RequestId,
								},
							}
							kv.mu.Lock()
							kv.rf.Start(command)
							kv.mu.Unlock()
							return
						}
					}
				}
			} (shardId)
			
		}
		wg.Wait()
		//log.Printf("%d updateShard one loop finish...", kv.gid)
		time.Sleep(50 * time.Millisecond)
	}
}

type PullArgs struct {
	Num		int
	Shard	int
	Gid		int	
}

type PullReply struct {
	Err		Err
	KV		map[string]string
	RequestId	map[int64]int64
}

func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.gid != args.Gid {
		reply.Err = ErrWrongGroup
		//log.Printf("%d get wrong gid %d pull", kv.gid, args.Gid)
		return
	}
	// 判断配置号是否满足
	if kv.currentConfig.Num != args.Num {
		reply.Err = ErrConfig
		//log.Printf("%d Pull ErrConfig %d", kv.gid, kv.currentConfig.Num)
		return
	}
	// 检查分片是否满足
	if kv.status[args.Shard] != Erasing {
		reply.Err = "ErrShardStatus"
		//log.Printf("%d Pull ErrShardStatus %d", kv.gid, kv.status[args.Shard])
		return
	}
	reply.Err = OK
	reply.KV = make(map[string]string)
	for key, value := range kv.gcShard[args.Shard] {
		reply.KV[key] = value
	}
	reply.RequestId = make(map[int64]int64)
	for clientId, requestId := range kv.clientRequestId {
		reply.RequestId[clientId] = requestId
	}
	//log.Printf("%d pull shard %dOK", kv.gid, args.Shard)
}

func (kv *ShardKV) checkShardStatus() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shard := range kv.status {
		if shard != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) gcShardsLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()
		if !isLeader {
			//log.Printf("%d gcShardLoop %d is not leader...", kv.gid, kv.me)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		shardIds := make([]int, 0)
		kv.mu.Lock()
		for shardId := range kv.gcShard {
			shardIds = append(shardIds, shardId)
		}
		kv.mu.Unlock()
		var wg sync.WaitGroup
		//log.Printf("%d gcShards Looping...", kv.gid)
		for _, shardId := range shardIds {
			//log.Printf("%d gcing %d", kv.gid, shardId)
			wg.Add(1)
			go func(shardId int) {
				defer wg.Done()
				kv.mu.Lock()
				gid := kv.currentConfig.Shards[shardId]
				args := CompleteArgs {
					Num: kv.currentConfig.Num,
					Shard: shardId,
					Gid: gid,
				}
				servers, ok := kv.currentConfig.Groups[gid];
				kv.mu.Unlock()
				if ok {
					for _, server := range servers {
						
						srv := kv.make_end(server)
						var reply CompleteReply
						ok := srv.Call("ShardKV.CheckComplete", &args, &reply)
						if !ok {
							//log.Printf("%d CheckComplete RPC fail!", kv.gid)
						}
						if ok && (reply.Err == OK) {
							command := Command {
								Op: DeleteShards,
								Data: GCShard{
									ShardId: shardId,
								},
							}
							//log.Printf("%d call CheckComplete %s successs", kv.gid, server)
							kv.mu.Lock()
							kv.rf.Start(command)
							kv.mu.Unlock()
							return
						} 
					}
				}
			}(shardId)
		}
		wg.Wait()
		//log.Printf("%d gcShardLoop one loop finish...", kv.gid)
		time.Sleep(50 * time.Millisecond)
	}
}

type CompleteArgs struct{
	Num		int
	Shard	int
	Gid		int
}

type CompleteReply struct {
	Err		Err
}

func (kv *ShardKV) CheckComplete(args *CompleteArgs, reply *CompleteReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 判断是否是该组
	if kv.gid != args.Gid {
		reply.Err = ErrWrongGroup
		//log.Printf("%d get wrong gid %d checkComplete", kv.gid, args.Gid)
		return
	}
	// 判断配置号是否满足
	if kv.currentConfig.Num < args.Num {
		reply.Err = ErrConfig
		//log.Printf("%d checkComplete ErrConfig %d", kv.gid, kv.currentConfig.Num)
		return
	}
	// 检查分片是否满足
	if kv.currentConfig.Num > args.Num || kv.status[args.Shard] == Serving {
		reply.Err = OK
	} else {
		reply.Err = ErrConfig
		//log.Printf("%d checkComplete ErrShardStatus %d", kv.gid, kv.status[args.Shard])
	}
}


func (kv *ShardKV)kvServerSnapShot() []byte{
	
	w := new(bytes.Buffer)
	e :=labgob.NewEncoder(w)

	if e.Encode(kv.store) != nil||
		e.Encode(kv.status) != nil ||
		e.Encode(kv.clientRequestId) != nil || 
		e.Encode(kv.lastApplied) != nil || 
		e.Encode(kv.lastConfig) != nil || 
		e.Encode(kv.currentConfig) != nil ||
		e.Encode(kv.pullingShard) != nil ||
		e.Encode(kv.gcShard) != nil {
			return nil
	}
	return w.Bytes()
}

func (kv *ShardKV)readSnapShot(data []byte){
	if len(data) == 0{
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store map[int]map[string]string
	var status map[int]int
	var	clientRequestId map[int64]int64
	var lastApplied int
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	var pullingShard map[int]map[string]string
	var gcShard map[int]map[string]string

	if d.Decode(&store) !=nil ||
		d.Decode(&status) != nil ||
		d.Decode(&clientRequestId) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&pullingShard) != nil ||
		d.Decode(&gcShard) != nil {
			return 
	}
	kv.store = store
	kv.status = status
	kv.clientRequestId = clientRequestId
	kv.lastApplied = lastApplied
	kv.lastConfig = lastConfig
	kv.currentConfig = currentConfig
	kv.pullingShard = pullingShard
	kv.gcShard = gcShard
}



func (kv *ShardKV) snapshotLoop() {
	for !kv.killed() && kv.maxraftstate != -1 {
		kv.mu.Lock()
		if kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.rf.Snapshot(kv.lastApplied, kv.kvServerSnapShot())
			//log.Printf("do snapshot %d", kv.persister.RaftStateSize())
		}
		kv.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
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
	
	for gid := range kv.currentConfig.Groups {
		kv.gid2Leader[gid] = 0
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
	kv.store = make(map[int]map[string]string)
	kv.status = make(map[int]int)
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
	kv.lastConfig = shardctrler.Config{}
	kv.currentConfig = shardctrler.Config{}
	kv.init()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.readSnapShot(snapshot)
	}
	go kv.applyLoop()
	go kv.checkConfigLoop()
	go kv.updateShardsLoop()
	go kv.gcShardsLoop()
	go kv.snapshotLoop()

	return kv
}
