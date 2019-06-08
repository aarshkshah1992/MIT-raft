package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// duration between election timeout polls
	sleepDuration = 50 * time.Millisecond
	// exercise mandates election of a new leader in 5s, even if there are multiple splitvotes. These values are based on testing.
	maxElectionTimeout = 1500
	minElectionTimeout = 1000
)

type electionHandler struct {
	// mutex to guard the next timeout
	mu   sync.Mutex
	raft *Raft
	// time at which the handler will timeout next
	electionTimeoutAt time.Time
}

// keep polling to see if election timeout has elapsed
func (e *electionHandler) start() {
	for {
		time.Sleep(sleepDuration)
		e.mu.Lock()
		electionTimeoutAt := e.electionTimeoutAt
		e.mu.Unlock()

		// election timeout has occurred
		if time.Now().After(electionTimeoutAt) {
			e.raft.mu.Lock()
			// act only if I am not the leader
			if e.raft.state != Leader {
				e.raft.becomeCandidate()
				// resetTimer election timer as a new election has started
				e.resetTimer()
				go e.sendRequestVoteRPCs()
			}
			e.raft.mu.Unlock()
		}
	}
}

func (e *electionHandler) sendRequestVoteRPCs() {
	// count self vote
	nVotes := 1

	// ask all peers for votes
	for peer, _ := range e.raft.peers {
		if peer == e.raft.me {
			continue

		}

		go func(rpcTerm int, peer int) {
			e.raft.mu.Lock()
			lastlogIndex := e.raft.lastLogIndex()
			lastLogTerm := e.raft.lastLogTerm()
			req := &RequestVoteArgs{rpcTerm, e.raft.me, lastlogIndex, lastLogTerm}
			e.raft.mu.Unlock()

			// send rpc & wait for reply
			reply := &RequestVoteReply{}
			ok := e.raft.sendRequestVote(peer, req, reply)

			e.raft.mu.Lock()
			defer e.raft.mu.Unlock()

			// don't process vote if rpc failed or my state/term changed since sending RPC
			if !ok || e.raft.currentTerm != rpcTerm || e.raft.state != Candidate {
				return
			}

			// become a follower if I am on an older term
			if reply.Term > e.raft.currentTerm {
				e.raft.becomeFollowerForTerm(reply.Term)
				return
			}

			// vote has been granted
			if reply.VoteGranted {
				nVotes = nVotes + 1
				// transition to leader if election has been won
				if nVotes > len(e.raft.peers)/2 {
					e.raft.becomeLeader()
				}
			}
		}(e.raft.currentTerm, peer)
	}
}

func (e *electionHandler) resetTimer() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.electionTimeoutAt = time.Now().Add(getNewElectionTimeout())
}

func getNewElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(maxElectionTimeout+1-minElectionTimeout)+minElectionTimeout) * time.Millisecond
}
