package raft

import "time"

const (
	// exercise only allows 10 heartbeats per second
	heartbeatInterval = 100 * time.Millisecond
)

// send periodic heartbeats till my current term is same as the term I became the leader for
func (rf *Raft) scheduleHeartbeats(leadershipTerm int) {
	ticker := time.NewTicker(heartbeatInterval)
	for _ = range ticker.C {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		// return if term has changed
		if currentTerm != leadershipTerm {
			ticker.Stop()
			return
		}

		rf.sendHeartBeatRPCs(leadershipTerm)
	}
}

// send heartbeat to all my followers
func (rf *Raft) sendHeartBeatRPCs(leadershipTerm int) {
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			lastlogIndex := rf.lastLogIndex()
			lastLogTerm := rf.log[lastlogIndex].Term
			heartbeat := &AppendEntriesArgs{Term: leadershipTerm, LeaderId: rf.me, PrevLogIndex: lastlogIndex,
				PrevLogTerm: lastLogTerm, LeaderCommit: rf.commitIndex}

			rf.mu.Unlock()

			// send RPC & wait for reply
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, heartbeat, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// don't process reply if rpc failed or my state/term changed since sending RPC
			if !ok || leadershipTerm != rf.currentTerm {
				return
			}

			// become a follower if I am on an older term
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerForTerm(reply.Term)
				return
			}
		}(peer)
	}
}
