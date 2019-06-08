package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"MIT-6.824/6.824/src/labgob"
	"MIT-6.824/6.824/src/labrpc"
	"github.com/pkg/errors"
)

var ErrNoStatePersisted = errors.New("no previous raft state found")

// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persistent state
	currentTerm int // my current term, starts from 0
	votedFor    int // index into peers[], -1 means nil
	log         []LogEntry

	// volatile state on all servers
	state       State // inits to follower
	commitIndex int   //index of the highest log entry that I have committed, init to 0
	lastApplied int   // index of highest log entry that I have applied to state machine, init to 0

	// volatile state on leader
	leaderState *LeaderState // reinit when I am elected as leader

	// helpers
	elecTimeoutHandler electionHandler // handles election timeouts, asks for votes etc

	// Channel for sending committed commands
	applyCh chan ApplyMsg

	// Channel to signal commit index changes
	commitIndexChan chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// refuse vote if candidate is on a lower term
	if args.Term < rf.currentTerm {
		rf.refuseVote(reply)
		return
	}

	// become follower if candidate is on higher term & continue processing RPC
	if args.Term > rf.currentTerm {
		rf.becomeFollowerForTerm(args.Term)
	}

	// terms match -> check if I can still vote & that candidate's log is up to date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isCandidateLogUpToDate(args) {
		rf.grantVote(args, reply)

		// resetTimer election timer as I granted a vote
		rf.elecTimeoutHandler.resetTimer()
	} else {
		// refuse vote as either I have already voted or candidate's log is not up to date
		rf.refuseVote(reply)
	}
}

// Append Entries Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if sender is on a lower term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// ----Can now assume that we are dealing with RPC from current leader-----------------------------------------------

	// Reset timer as we heard from current leader
	rf.elecTimeoutHandler.resetTimer()

	// become follower if I am on lower term & continue processing RPC
	if args.Term > rf.currentTerm {
		rf.becomeFollowerForTerm(args.Term)
	}

	// return false if I do NOT have the preceding entry sent by the leader
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		rf.replyAppendCallByCurrentLeader(reply, false)
		return
	}

	// return false if terms of the preceding entry do not match
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		for i := 1; i <= rf.lastLogIndex(); i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}

		rf.replyAppendCallByCurrentLeader(reply, false)
		return
	}

	// 1) starting after the prev matching entry, remove entries in my log that conflict with the leader's log
	// 2) append entries sent by the leader which are absent in my log
	lastNewEntryIndex, logChange := rf.syncLogs(args)
	if logChange {
		rf.persist()
	}

	// update my commit index if leader has committed more entries than me
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}

		// tell SM applier that my commitIndex has changed
		go func() {
			rf.commitIndexChan <- struct{}{}
		}()
	}

	rf.replyAppendCallByCurrentLeader(reply, true)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	go rf.applyToStateMachine()

	// append to log
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist()
	return rf.lastLogIndex(), rf.currentTerm, true
}

func (rf *Raft) applyToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// send append entries to all followers
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}

		// do I need to send the entry to this peer ?
		if rf.lastLogIndex() >= rf.leaderState.nextIndex[peer] {
			go rf.sendAppendEntriesRPC(rf.currentTerm, peer)
		}
	}
}

func (rf *Raft) sendAppendEntriesRPC(leadershipTerm int, peer int) {
	rf.mu.Lock()
	nextIndex := rf.leaderState.nextIndex[peer]
	prevIndex := nextIndex - 1
	prevTerm := rf.log[prevIndex].Term
	entries := rf.log[nextIndex : rf.lastLogIndex()+1]
	appendEntriesArgs := &AppendEntriesArgs{Term: leadershipTerm, LeaderId: rf.me, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries,
		LeaderCommit: rf.commitIndex}

	rf.mu.Unlock()

	// send RPC & wait for reply
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, appendEntriesArgs, reply)

	// reacquire lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// return if my term changed since sending the RPC
	if rf.currentTerm != leadershipTerm {
		return
	}

	// retry if RPC failed
	if !ok {
		go rf.sendAppendEntriesRPC(leadershipTerm, peer)
		return
	}

	// become follower & return if I see a higher term
	if reply.Term > leadershipTerm {
		rf.becomeFollowerForTerm(reply.Term)
		return
	}

	// current term, term on rpc & term on follower match -> process reply
	if reply.Success {
		rf.leaderState.matchIndex[peer] = prevIndex + len(entries)
		rf.leaderState.nextIndex[peer] = rf.leaderState.matchIndex[peer] + 1

		// update the commit index if applicable
		go rf.updateLeaderCommitIndex(leadershipTerm)
		return
	}

	// follower's log is not in sync, try to find an older matching entry
	if reply.ConflictTerm != -1 {
		for i := rf.lastLogIndex(); i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				rf.leaderState.nextIndex[peer] = i + 1
				go rf.sendAppendEntriesRPC(leadershipTerm, peer)
				return
			}
		}
	}

	rf.leaderState.nextIndex[peer] = reply.ConflictIndex
	go rf.sendAppendEntriesRPC(leadershipTerm, peer)
}

//  Update the leader's commit index to the highest index that has been replicated on a majority of servers for the CURRENT TERM
func (rf *Raft) updateLeaderCommitIndex(leadershipTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	newCommitIndex := rf.commitIndex

	for index := rf.commitIndex + 1; index <= rf.lastLogIndex(); index++ {
		replicationCount := 1

		// check if entry at index=index has been replicated on majority peers in the current term
		for peer, _ := range rf.peers {
			if peer == rf.me {
				continue
			}

			if rf.leaderState.matchIndex[peer] >= index && rf.log[index].Term == leadershipTerm {
				replicationCount = replicationCount + 1
				// majority achieved
				if replicationCount > len(rf.peers)/2 {
					newCommitIndex = index
					break
				}
			}
		}
	}

	// update commit index & signal to SM applier that commit index has changed
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		go func() {
			rf.commitIndexChan <- struct{}{}
		}()
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// imp to seed the rand lib
	rand.Seed(time.Now().UTC().UnixNano())

	// init Raft structure
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.commitIndexChan = make(chan struct{})
	rf.state = Follower

	// init volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	err := rf.readPersist(persister.ReadRaftState())
	if err != nil {
		if err == ErrNoStatePersisted {
			rf.currentTerm = 0
			rf.votedFor = -1
			rf.log = make([]LogEntry, 1)
			rf.log[0] = LogEntry{0, nil}
		} else {
			panic(err)
		}
	}

	// create election handler & start a dedicated go-routine to handle election timeouts
	rf.elecTimeoutHandler = electionHandler{raft: rf, electionTimeoutAt: time.Now().Add(getNewElectionTimeout())}
	go rf.elecTimeoutHandler.start()

	// dedicated go routine to apply committed commands
	go rf.applyCommittedCommands()

	return rf
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) error {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return ErrNoStatePersisted
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return errors.New("failed to read persisted state")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	return nil
}

func (rf *Raft) applyCommittedCommands() {
	for {
		// block till commit index changes
		<-rf.commitIndexChan

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		// apply all commands between (lastApplied, commitIndex]
		for index := lastApplied + 1; index <= commitIndex; index++ {
			rf.mu.Lock()
			entry := rf.log[index]
			applyMsg := ApplyMsg{CommandValid: true, Command: entry.Cmd, CommandIndex: index}
			// release lock as writing on channel can block
			rf.mu.Unlock()

			rf.applyCh <- applyMsg

			rf.mu.Lock()
			rf.lastApplied = rf.lastApplied + 1
			rf.mu.Unlock()
		}

	}
}
