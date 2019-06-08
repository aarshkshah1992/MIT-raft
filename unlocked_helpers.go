package raft

import (
	"bytes"

	"MIT-6.824/6.824/src/labgob"
)

/* HANDLE LOCKING IN THE CALLER FOR ALL FUNCTIONS HERE*/

func (rf *Raft) becomeFollowerForTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) grantVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.persist()
}

func (rf *Raft) refuseVote(reply *RequestVoteReply) {
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

func (rf *Raft) replyAppendCallByCurrentLeader(reply *AppendEntriesReply, success bool) {
	reply.Term = rf.currentTerm
	reply.Success = success
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	// init leader state
	rf.leaderState = &LeaderState{}
	for i := 0; i < len(rf.peers); i++ {
		rf.leaderState.nextIndex = append(rf.leaderState.nextIndex, rf.lastLogIndex()+1)
		rf.leaderState.matchIndex = append(rf.leaderState.matchIndex, 0)
	}

	// send heartbeats immediately & then schedule at regular intervals
	rf.sendHeartBeatRPCs(rf.currentTerm)
	go rf.scheduleHeartbeats(rf.currentTerm)

}

func (rf *Raft) isCandidateLogUpToDate(args *RequestVoteArgs) bool {
	if args.LastLogTerm == rf.lastLogTerm() {
		return args.LastLogIndex >= rf.lastLogIndex()
	}
	return args.LastLogTerm > rf.lastLogTerm()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

func (rf *Raft) syncLogs(args *AppendEntriesArgs) int {
	// index into the 'new' entries sent by the leader as a part of Append Entries
	leaderEntriesIndex := 0
	// index into my own log starting from the matching entry + 1
	logIndex := args.PrevLogIndex + 1

	// find index for which there is a conflict & delete all entries from then onwards from my log
	for leaderEntriesIndex < len(args.Entries) && logIndex <= rf.lastLogIndex() {
		if rf.log[logIndex].Term != args.Entries[leaderEntriesIndex].Term {
			// conflicting entry...delete all entries in the log starting from that index
			rf.log = rf.log[:logIndex]
			break
		}
		leaderEntriesIndex++
		logIndex++
	}

	// append entries into my log that I do not have
	for leaderEntriesIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[leaderEntriesIndex])
		leaderEntriesIndex++
		logIndex++
	}

	return logIndex - 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode curr term
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}

	// encode voted for
	err = e.Encode(rf.votedFor)
	if err != nil {
		panic(err)
	}

	// encode rf log
	err = e.Encode(rf.log)
	if err != nil {
		panic(err)
	}

	rf.persister.SaveRaftState(w.Bytes())
}
