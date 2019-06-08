package raft

// State is the current state of the Raft instance
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term int
	Cmd  interface{}
}

type LeaderState struct {
	nextIndex  []int // index of next entry to send to a follower
	matchIndex []int // index of highest entry replicated on follower
}
