package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId	int
	clientId	int64
	requestId	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.requestId++
	args := &QueryArgs{
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestId++
	args := &JoinArgs{
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestId++
	args := &LeaveArgs{
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestId++
	args := &MoveArgs{
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
