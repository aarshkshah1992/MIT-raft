package raftkv

import (
	"context"
	"log"
	"sync"
	"time"

	"MIT-6.824/6.824/src/labgob"
	"MIT-6.824/6.824/src/labrpc"
	raft "MIT-6.824/6.824/src/raft/MIT-raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op is the cmd sent to raft
type Op struct {
	Name     string
	Key      string
	Value    string
	ClientID int64
	ReqID    int64
}

type Response struct {
	OP    Op
	Value string // for get requests
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	state               map[string]string
	commitNotifications map[int][]chan *Response
	latestReqs          map[int64]int64 // last seen reqID for each client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{Name: GetOp, Key: args.Key, ClientID: args.ClientID, ReqID: args.ReqID}

	// submit operation to Raft
	isWrongLeader, err, value := kv.submitReq(op)

	// return if wrong leader or error
	if isWrongLeader || len(err) != 0 {
		reply.WrongLeader = isWrongLeader
		reply.Err = Err(err)
		return
	}

	// no such key exists
	if len(value) == 0 {
		reply.Err = ErrNoKey
		return
	}

	// all good
	reply.Err = OK
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Name: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, ReqID: args.ReqID}

	// submit operation to Raft
	isWrongLeader, err, _ := kv.submitReq(op)

	// return if wrong leader or error
	if isWrongLeader || len(err) != 0 {
		reply.WrongLeader = isWrongLeader
		reply.Err = Err(err)
		return
	}

	// all good
	reply.Err = OK
}

func (kv *KVServer) submitReq(op Op) (bool, string, string) {
	kv.mu.Lock()

	// submit op to raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return true, "", ""
	}

	// subscribe for notification when entry at this index is committed
	responseChan := make(chan *Response, 1)
	kv.commitNotifications[index] = append(kv.commitNotifications[index], responseChan)
	kv.mu.Unlock()

	// this channel will be written to when server's term changes
	termChangeChan := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go kv.notifyIfTermChange(ctx, term, termChangeChan)

	// block till we get a response for the op or server's term changes
	select {
	case res := <-responseChan:
		// ensure it is the req we were expecting
		if !isExpectedResponse(res, op.ClientID, op.ReqID) {
			return false, ErrReqMismatch, ""
		}
		return false, "", res.Value
	case <-termChangeChan:
		return false, ErrReqMismatch, ""
	}
}

func (kv *KVServer) notifyIfTermChange(ctx context.Context, termForReq int, termChangeChan chan struct{}) {
	t := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			close(termChangeChan)
			return
		case <-t.C:
			currentTerm, _ := kv.rf.GetState()
			if currentTerm != termForReq {
				termChangeChan <- struct{}{}
				close(termChangeChan)
				t.Stop()
				return
			}
		}
	}
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
}

func isExpectedResponse(res *Response, clientID, reqID int64) bool {
	return res.OP.ReqID == reqID && res.OP.ClientID == clientID
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
	kv.state = make(map[string]string)
	kv.commitNotifications = make(map[int][]chan *Response)
	kv.latestReqs = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start applier thread
	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for {
		// wait for a command to be committed
		applyMsg := <-kv.applyCh
		if !applyMsg.CommandValid {
			continue
		}

		// get op & make response
		op := applyMsg.Command.(Op)
		resp := &Response{}
		resp.OP = op

		// update state machine
		kv.mu.Lock()
		switch op.Name {
		case GetOp:
			resp.Value = kv.state[op.Key]
		case PutOp:
			// apply only if not seen before
			if kv.notSeen(&op) {
				kv.state[op.Key] = op.Value
				kv.latestReqs[op.ClientID] = op.ReqID
			}
		case AppendOp:
			// apply only if not seen before
			if kv.notSeen(&op) {
				kv.state[op.Key] = kv.state[op.Key] + op.Value
				kv.latestReqs[op.ClientID] = op.ReqID
			}
		}

		index := applyMsg.CommandIndex
		listeners := kv.commitNotifications[index]

		kv.mu.Unlock()
		// notify all listeners

		for _, listener := range listeners {
			listener <- resp
		}

	}
}

func (kv *KVServer) notSeen(op *Op) bool {
	return kv.latestReqs[op.ClientID] < op.ReqID
}
