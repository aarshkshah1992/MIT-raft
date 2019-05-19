package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// duration between election timeout pool
	sleepDuration = 20 * time.Millisecond
	// exercise mandates election of a new leader in 5s, even if there are multiple splitvotes. These values are based on testing.
	maxElectionTimeout = 1500
	minElectionTimeout = 1000
)

type ElectionHandler struct {
	mu   sync.Mutex
	raft *Raft
	// time at which the election timer will fire ... unless reset before that
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
				// vote for myself
				nVotes := 1
				e.raft.becomeCandidate()

				// ask for votes from all peers other than myself
				for i, _ := range e.raft.peers {
					if i == e.raft.me {
						continue
					}

					go func(rpcTerm int, peer int) {
						reply := &RequestVoteReply{}
						req := &RequestVoteArgs{rpcTerm, e.raft.me, 0, 0}
						// send rpc to ask for vote
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
							e.processVote(nVotes)
						}
					}(e.raft.currentTerm, i)
				}
			}

			e.raft.mu.Unlock()
		}
	}
}

func (e *ElectionHandler) processVote(nVotes int) {
	nVotes = nVotes + 1
	// have i won the election ?
	if nVotes > len(e.raft.peers)/2 {
		e.raft.becomeLeader()
	}
}

func (e *ElectionHandler) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fireAt = time.Now().Add(getNewElecTimeout())
}

// get a new random election timeout
func getNewElecTimeout() time.Duration {
	return time.Duration(rand.Intn(maxElectionTimeout+1-minElectionTimeout)+minElectionTimeout) * time.Millisecond
}
