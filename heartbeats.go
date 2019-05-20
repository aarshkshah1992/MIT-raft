package raft

import "time"

const (
	// exercise only allows 10 heartbeats per second
	heartbeatInterval = 100 * time.Millisecond
)

// this function schedules heartbeats to be sent periodically by me after I become leader
// it keeps sending heartbeats till my current term is same as the term I became the leader for
func (rf *Raft) scheduleHeartbeats(leadershipTerm int) {
	ticker := time.NewTicker(heartbeatInterval)
	for _ = range ticker.C {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		// ensure my term hasn't change since I became leader -> if it has, do not send heartbeats
		if currentTerm != leadershipTerm {
			ticker.Stop()
			return
		}

		rf.sendHeartBeatsToAllFollowers(leadershipTerm)
	}
}

// send heartbeat to all my followers
func (rf *Raft) sendHeartBeatsToAllFollowers(leadershipTerm int) {
	for peer, _ := range rf.peers {
		// do not send heartbeat to myself
		if peer == rf.me {
			continue
		}

		// send each heartbeat in it's own go-routine
		go func(peer int) {
			// make heartbeat msg
			rf.mu.Lock()
			lastlogIndex := rf.lastLogIndex()
			lastLogTerm := rf.log[lastlogIndex].Term
			heartbeat := &AppendEntriesArgs{Term: leadershipTerm, LeaderId: rf.me, PrevLogIndex: lastlogIndex, PrevLogTerm: lastLogTerm, LeaderCommit: rf.commitIndex}
			rf.mu.Unlock()

			// send heartbeat RPC & wait for reply
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, heartbeat, reply)

			// reacquire lock
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// assert that things haven't changed since I sent the RPC
			// RPC failed/my term changed -> do NOTHING
			if !ok || leadershipTerm != rf.currentTerm {
				return
			}

			// reply.Term > myterm -> become follower for new term
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerForTerm(reply.Term)
				return
			}
		}(peer)
	}
}
