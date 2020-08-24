package raft

import (
	"fmt"
	"time"
)

///// Append Entries RPC ///////
type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId int
	// TODO: add Log related stuff
}

type AppendEntriesReply struct {
	Term int
	Success bool
}


func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// case where we don't grant the vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("Server: %d (term: %d) recieved AppendEntry from Leader: %d (term: %d)\n", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm)
	if (args.LeaderTerm >= rf.currentTerm) {
		rf.curState = Follower
		rf.lastVisitedTime = time.Now() // this server is visited by a leader or candidate. Update this
		rf.currentTerm = args.LeaderTerm

		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}