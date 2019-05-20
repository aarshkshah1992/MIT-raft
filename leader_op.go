package raft

type LeaderState struct {
	nextIndex  []int // index of next entry to send to a follower
	matchIndex []int // index of highest entry replicated on server
}
