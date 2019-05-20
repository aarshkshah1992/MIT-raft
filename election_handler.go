package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// duration between election timeout pool
	sleepDuration = 50 * time.Millisecond
	// exercise mandates election of a new leader in 5s, even if there are multiple splitvotes. These values are based on testing.
	maxElectionTimeout = 1500
	minElectionTimeout = 1000
)

type ElectionHandler struct {
	// mutex to guard the next timeout
	mu   sync.Mutex
	raft *Raft
	// time at which the election timer will fire ... unless resetTimer before that
	fireAt time.Time
}

// keep polling to see if election timeout has elapsed
func (e *ElectionHandler) Start() {
	for {
		time.Sleep(sleepDuration)
		e.mu.Lock()
		currentFireAt := e.fireAt
		e.mu.Unlock()

		// election timeout has occurred
		if time.Now().After(currentFireAt) {
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

func (e *ElectionHandler) sendRequestVoteRPCs() {
	nVotes := 1

	// ask all peers for votes
	for peer, _ := range e.raft.peers {
		if peer == e.raft.me {
			continue

		}
		go func(rpcTerm int, peer int) {
			e.raft.mu.Lock()
			lastlogIndex := e.raft.lastLogIndex()
			lastLogTerm := e.raft.log[lastlogIndex].Term
			req := &RequestVoteArgs{rpcTerm, e.raft.me, lastlogIndex, lastLogTerm}
			e.raft.mu.Unlock()

			// send rpc to ask for vote
			reply := &RequestVoteReply{}
			ok := e.raft.sendRequestVote(peer, req, reply)

			e.raft.mu.Lock()
			defer e.raft.mu.Unlock()

			// assert that things haven't changed since I sent the RPC
			// RPC failed/my term/state changed -> do NOTHING
			if !ok || e.raft.currentTerm != rpcTerm || e.raft.state != Candidate {
				return
			}

			// reply.term > myterm -> become follower for new term &
			if reply.Term > e.raft.currentTerm {
				// Become follower
				e.raft.becomeFollowerForTerm(reply.Term)
				return
			}

			// vote has been granted
			if reply.VoteGranted {
				nVotes = nVotes + 1
				// have peer won the election ?
				if nVotes > len(e.raft.peers)/2 {
					e.raft.becomeLeader()
				}
			}
		}(e.raft.currentTerm, peer)
	}
}

func (e *ElectionHandler) resetTimer() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fireAt = time.Now().Add(getNewElectionTimeout())
}

func getNewElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(maxElectionTimeout+1-minElectionTimeout)+minElectionTimeout) * time.Millisecond
}
