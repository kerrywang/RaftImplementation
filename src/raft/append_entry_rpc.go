package raft

import (
	"time"
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

	// TODO: Implement next index optimization
	NextIndex int
}

func (rf *Raft) AppendEntryRequestSnapshot() (*AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Append Entry Snapshot has Commit ID: %d\n", rf.volatile_all_server.commitIndex)

	append_entry_request := &AppendEntriesArgs{}
	append_entry_request.LeaderId = rf.me
	append_entry_request.LeaderTerm = rf.persitent_state.currentTerm
	//append_entry_request.LeaderCommit = rf.volatile_all_server.commitIndex
	return append_entry_request
}

func (rf *Raft) AssembleAppendEntriesRequest(server_id int, snapshot_args *AppendEntriesArgs) (*AppendEntriesArgs)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Sever: %v Is Assembling messages to %d\n", rf.me, server_id)

	append_entry_request := &AppendEntriesArgs{}
	append_entry_request.LeaderId = snapshot_args.LeaderId
	append_entry_request.LeaderTerm = snapshot_args.LeaderTerm
	append_entry_request.LeaderCommit = rf.volatile_all_server.commitIndex
	append_entry_request.PrevLogIndex = rf.volatile_leader.nextIndex[server_id] - 1

	append_entry_request.PrevLogTerm = rf.persitent_state.logs[append_entry_request.PrevLogIndex].Term
	append_entry_request.Entries = rf.persitent_state.logs[rf.volatile_leader.nextIndex[server_id]:]

	rf.persist()
	return append_entry_request
}


func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) FindFirstIndexForTerm(term int) int {
	for i:= 0; i <= rf.getLastIndex(); i++ {
		if rf.persitent_state.logs[i].Term == term {
			return i
		}
	}
	return rf.getLastIndex()
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// case where we don't grant the vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server: %d (term: %d) (Commited LogIDX: %d  LOG INDEX: %d Log Term: %d) recieved AppendEntry from Leader: %d (term: %d) (Commited LogIDX: %d, PrevIndex: %d  PrevTerm: %d) Entries: %d\n", rf.me, rf.persitent_state.currentTerm, rf.volatile_all_server.commitIndex, rf.getLastIndex(), rf.getLastTerm(), args.LeaderId, args.LeaderTerm, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	DPrintf("Server: %d has log: %v, Incomming Log: %v\n", rf.me, rf.persitent_state.logs, args.Entries)
	rf.maybeUpdateTerm(args.LeaderTerm)

	//  Reply false if term < currentTerm (§5.1)
	if (args.LeaderTerm < rf.persitent_state.currentTerm) {
		reply.Term = rf.persitent_state.currentTerm
		reply.Success = false
		DPrintf("Append Entry Faild due to Leader Term Smaller than current Term")

		return
	}

	rf.last_synced_time = time.Now() // this server is visited by a leader or candidate. Update this


	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	if (rf.getLastIndex() < args.PrevLogIndex || rf.persitent_state.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.persitent_state.currentTerm
		reply.Success = false
		if rf.getLastIndex() < args.PrevLogIndex {
			reply.NextIndex = rf.getLastIndex()
		} else {
			reply.NextIndex = rf.FindFirstIndexForTerm(rf.persitent_state.logs[args.PrevLogIndex].Term)
		}
		//rf.persitent_state.votedFor = -1

		DPrintf("Append Entry Faild due to no matching log")
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
}