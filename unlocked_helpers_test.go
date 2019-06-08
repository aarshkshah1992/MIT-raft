package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncLogs(t *testing.T) {
	// Case 1 -> I have all entries that leader sends and some entries after that
	rf := &Raft{}
	log := []LogEntry{{0, nil}, {1, []byte("a")}, {2, []byte("b")},
		{3, []byte("c")}, {4, []byte("d")}}
	rf.log = log
	args := &AppendEntriesArgs{PrevLogIndex: 0, Entries: rf.log[1:4]}
	lastNewIndex, sc := rf.syncLogs(args)
	assert.Equal(t, 3, lastNewIndex)
	assert.Equal(t, log, rf.log)
	assert.False(t, sc)

	// Case 2 -> I have conflicting entries with the leader & leader has entries I don't
	rf.log = []LogEntry{{0, nil}, {1, []byte("a")}, {2, []byte("b")},
		{3, []byte("c")}, {4, []byte("d")}, {5, []byte("e")}}

	args = &AppendEntriesArgs{PrevLogIndex: 0, Entries: []LogEntry{{1, []byte("a")}, {3, []byte("b")},
		{4, []byte("c")}}}

	lastNewIndex, sc = rf.syncLogs(args)
	assert.Equal(t, 3, lastNewIndex)
	assert.Equal(t, append([]LogEntry{{0, nil}}, args.Entries...), rf.log)
	assert.True(t, sc)
}
