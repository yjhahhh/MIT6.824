package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

// 每个客户端通过Clerk与服务器交流
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id			int64
	leaderId	int
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
	// You'll have to add code here.

	ck.id = nrand();
	ck.requestId = 0;
	ck.leaderId = 0;
	//log.Printf("MakeClear...")
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestId++
	args := GetArgs {
		Key: key,
		ClientId: ck.id,
		RequestId: ck.requestId,
	}

	for {
		reply := GetReply{}
		//log.Printf("%d send Get...", ck.id)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			} else {
				return reply.Value
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs {
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.id,
		RequestId: ck.requestId,
	}

	for {
		reply := PutAppendReply{}
		//log.Printf("%d send PutAppend...", ck.id)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				//log.Printf("server error in send PutAppend")
			} else {
				return
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.requestId++
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.requestId++
	ck.PutAppend(key, value, "Append")
}
