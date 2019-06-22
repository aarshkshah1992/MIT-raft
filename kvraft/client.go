package raftkv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"MIT-6.824/6.824/src/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentLeader int64
	clerkID       int64
	currentOffset int64
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
	ck.clerkID = nrand()
	return ck
}

func (ck *Clerk) getOffset() int64 {
	return atomic.AddInt64(&ck.currentOffset, 1)
}

func (ck *Clerk) Get(key string) string {
	currentLeader := atomic.LoadInt64(&ck.currentLeader)
	hasChanged := false
	id := ck.getOffset()

	for {
		req := &GetArgs{Key: key, ClientID: ck.clerkID, ReqID: id}
		reply := &GetReply{}
		ok := ck.servers[currentLeader].Call("KVServer.Get", req, reply)
		// server not reachable or wrong leader
		if ok && !reply.WrongLeader && (reply.Err == ErrNoKey || reply.Err == OK) {
			// update current leader
			if hasChanged {
				atomic.StoreInt64(&ck.currentLeader, currentLeader)
			}
			return reply.Value
		}
		hasChanged = true
		currentLeader = (currentLeader + 1) % int64(len(ck.servers))
	}

	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	currentLeader := atomic.LoadInt64(&ck.currentLeader)
	hasChanged := false
	id := ck.getOffset()

	for {
		req := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clerkID, ReqID: id}
		reply := &PutAppendReply{}
		ok := ck.servers[currentLeader].Call("KVServer.PutAppend", req, reply)

		// receieved successful response from leader
		if ok && !reply.WrongLeader && reply.Err == OK {
			// update current leader
			if hasChanged {
				atomic.StoreInt64(&ck.currentLeader, currentLeader)
			}
			return
		}

		hasChanged = true
		currentLeader = (currentLeader + 1) % int64(len(ck.servers))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
