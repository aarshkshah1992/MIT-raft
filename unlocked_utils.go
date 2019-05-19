package raft

/* HANDLE LOCKING IN THE CALLER FOR ALL FUNCTIONS HERE*/

func (rf *Raft) becomeFollowerForTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
}

func (rf *Raft) grantVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID

	// reset election timer as I granted a vote
	rf.elecTimeoutHandler.reset()
}

func (rf *Raft) refuseVote(reply *RequestVoteReply) {
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

func (rf *Raft) replyAppendCallByCurrentLeader(reply *AppendEntriesReply, success bool) {
	rf.elecTimeoutHandler.reset()

	reply.Term = rf.currentTerm
	reply.Success = success
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me

	rf.elecTimeoutHandler.reset()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	// init leader state
	rf.leaderState = &LeaderState{}
	for i := 0; i < len(rf.peers); i++ {
		rf.leaderState.nextIndex = append(rf.leaderState.nextIndex, rf.lastLogIndex()+1)
		rf.leaderState.matchIndex = append(rf.leaderState.matchIndex, 0)
	}

	// send heartbeats immediately & then  schedule at regular intervals
	rf.sendHeartBeatsToAllFollowers(rf.currentTerm)
	go rf.scheduleHeartbeats(rf.currentTerm)
}

func (rf *Raft) isCandidateLogUpToDate(args *RequestVoteArgs) bool {
	return args.LastLogTerm >= rf.log[rf.lastLogIndex()].Term && args.LastLogIndex >= rf.lastLogIndex()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) syncLogs(args *AppendEntriesArgs) int {
	leaderEntriesIndex := 0
	logIndex := args.PrevLogIndex + 1

	// find index into both for which there is a conflict & delete the conflicting entries from my log
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
