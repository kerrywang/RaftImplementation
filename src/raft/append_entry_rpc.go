package raft

import (
	//"time"
)

/* Invoked by leader to replicate log entries; Also used as heartbeat */

///// Append Entries RPC ///////
type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId int
	// TODO: add Log related stuff
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int

	Entries []Log // log entries to store, empty for heartbeat; may send more than one for efficiency
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

	DPrintf("Server: %d (term: %d) (Commited LogIDX: %d  LOG INDEX: %d Log Term: %d) recieved AppendEntry from Leader: %d (term: %d) (Commited LogIDX: %d, PrevIndex: %d  PrevTerm: %d) Entries: %d\n", rf.me, rf.currentTerm, rf.commitIndex, rf.getLastIndex(), rf.getLastTerm(), args.LeaderId, args.LeaderTerm, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	//  Reply false if term < currentTerm (§5.1)
	if (args.LeaderTerm < rf.persitent_state.currentTerm) {
		reply.Term = rf.persitent_state.currentTerm
		reply.Success = false
		//rf.persitent_state.votedFor = -1
		DPrintf("Append Entry Faild due to Leader Term Smaller than current Term")

		rf.maybeUpdateTerm(args.LeaderTerm)
		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	if (rf.getLastIndex() < args.PrevLogIndex || rf.persitent_state.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.persitent_state.currentTerm
		reply.Success = false
		//rf.persitent_state.votedFor = -1
		DPrintf("Append Entry Faild due to no matching log")
		rf.maybeUpdateTerm(args.LeaderTerm)
		return
	}


	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	for i := 0; i < len(args.Entries); i++ {
		iter_in_log := i + args.PrevLogIndex + 1
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		if (rf.getLastIndex() >= iter_in_log && rf.persitent_state.logs[iter_in_log].Term != args.Entries[i].Term) {
			rf.persitent_state.logs = rf.persitent_state.logs[:iter_in_log]
		}

		// Append any new entries not already in the log
		if (rf.getLastIndex() < iter_in_log) {
			rf.persitent_state.logs = append(rf.persitent_state.logs, args.Entries[i])
		}
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if (args.LeaderCommit > rf.volatile_all_server.commitIndex) {
		rf.volatile_all_server.commitIndex = MIN(rf.getLastIndex(), args.LeaderCommit)
	}
	reply.Term = rf.persitent_state.currentTerm
	reply.Success = true

	DPrintf("Finished Append: Server: %v Last Term: %d", rf.me, rf.getLastTerm())
	rf.persist()
	rf.maybeUpdateTerm(args.LeaderTerm)

	//// update current status because it is leader sending message
	////if (args.LeaderTerm >= rf.currentTerm) {
	//rf.curState = Follower
	//rf.lastVisitedTime = time.Now() // this server is visited by a leader or candidate. Update this
	//
	////DPrintf("Server: %d updates its Last Visited Time: %d\n", rf.me, rf.lastVisitedTime .Sub(rf.initializedTime).Milliseconds())
	//reply.Term = rf.currentTerm
	//rf.currentTerm = args.LeaderTerm
	//
	//
	////  Reply false if log doesn’t contain an entry at prevLogIndex
	////if (rf.getLastIndex() < args.PrevLogIndex) {
	////	reply.Success = false
	////	reply.NextIndex = rf.getLastIndex() + 1
	////	rf.persist()
	////	return
	////}
	//// Reply false if log entry has different term at pervLogIndex
	//if (rf.getLastIndex() < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
	//	reply.Success = false
	//
	//	// apply fast log back off
	//	for i := MIN(rf.getLastIndex(), args.PrevLogIndex - 1); i >= 0; i-- {
	//		if (rf.logs[i].Term != args.PrevLogTerm) {
	//			reply.NextIndex = i + 1
	//			break
	//		}
	//	}
	//	rf.persist()
	//	DPrintf("Append Entry Faild due to mismatched data")
	//	return
	//}
	//
	//// Successful case
	//reply.Success = true
	//for i := 0; i < len(args.Entries); i++ {
	//	iter_in_log := i + args.PrevLogIndex + 1
	//	if (len(rf.logs) <= iter_in_log) {
	//		rf.logs = append(rf.logs, args.Entries[i])
	//	} else if (rf.logs[iter_in_log].Term != args.Entries[i].Term || rf.logs[iter_in_log].Pos != args.Entries[i].Pos) {
	//		rf.logs = rf.logs[:iter_in_log]
	//		rf.logs = append(rf.logs, args.Entries[i])
	//	}
	//}
	//
	//reply.NextIndex = rf.getLastIndex() + 1
	//
	////startCommitIndex := rf.commitIndex + 1
	////for i := startCommitIndex; i <= MIN(args.PrevLogIndex + len(args.Entries), args.LeaderCommit); i++ {
	////	rf.apply_chan <- ApplyMsg{
	////		CommandValid: true,
	////		Command:      rf.logs[i].Command,
	////		CommandIndex: i,
	////	}
	////
	////	rf.commitIndex = i
	////	DPrintf("Server: %v Commit_ID: %d Last Term: %d\n", rf.me, rf.commitIndex, rf.logs[i].Term)
	////	//DPrintf("Server: %v Total Log: %v\n", rf.me, rf.logs)
	////
	////}


	//rf.logs = append(rf.logs, args.Entries...)
	//DPrintf("Server: %d finisehd processing took Time: %d\n", rf.me, time.Now().Sub(rf.initializedTime).Milliseconds())

}