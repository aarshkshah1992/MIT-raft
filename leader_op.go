package raft

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}
