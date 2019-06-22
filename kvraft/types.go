package raftkv

const (
	GetOp          = "Get"
	PutOp          = "Put"
	AppendOp       = "Append"
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrReqMismatch = "ErrReqMismatch"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientID int64
	ReqID    int64
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	ClientID int64
	ReqID    int64
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
