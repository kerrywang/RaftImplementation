package raft

import (
	"time"
)

///// Append Entries RPC ///////
type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId int
	// TODO: add Log related stuff
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int

	Entries []Log
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
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

	DPrintf("Server: %d (term: %d) recieved AppendEntry from Leader: %d (term: %d) With Applied Entry: %v\n", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm, args.Entries)

	// if reply is not fit to be leader directly returns
	if (args.LeaderTerm < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastIndex()
		rf.votedFor = -1
		return
	}
	// update current status because it is leader sending message
	//if (args.LeaderTerm >= rf.currentTerm) {
	rf.curState = Follower
	rf.lastVisitedTime = time.Now() // this server is visited by a leader or candidate. Update this

	//DPrintf("Server: %d updates its Last Visited Time: %d\n", rf.me, rf.lastVisitedTime .Sub(rf.initializedTime).Milliseconds())

	rf.currentTerm = args.LeaderTerm

	reply.Term = rf.currentTerm

	//  Reply false if log doesn’t contain an entry at prevLogIndex
	if (rf.getLastIndex() < args.PrevLogIndex) {
		reply.Success = false
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	// Reply false if log entry has different term at pervLogIndex
	if (rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false

		// apply fast log back off
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if (rf.logs[i].Term != args.PrevLogTerm) {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	// Successful case
	reply.Success = true
	rf.logs = rf.logs[: args.PrevLogIndex + 1]
	rf.logs = append(rf.logs, args.Entries...)
	reply.NextIndex = rf.getLastIndex() + 1

	startCommitIndex := rf.commitIndex + 1
	for i := startCommitIndex; i <= MIN(rf.getLastIndex(), args.LeaderCommit); i++ {
		rf.apply_chan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}

		rf.commitIndex = i
		DPrintf("Server: %v Commit_ID: %d Commited Entry: %v Last Term: %d\n", rf.me, rf.commitIndex, rf.logs[i].Command, rf.logs[i].Term)
		//DPrintf("Server: %v Total Log: %v\n", rf.me, rf.logs)

	}
	DPrintf("Server: %v Last Term: %d", rf.me, rf.getLastTerm())
	//rf.logs = append(rf.logs, args.Entries...)
	//DPrintf("Server: %d finisehd processing took Time: %d\n", rf.me, time.Now().Sub(rf.initializedTime).Milliseconds())

}