package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int

	//term of candidate’s last log entry (§5.4)
	LastLogTerm int

	//index of candidate’s last log entry
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int
	VoteGranted bool
}

func (rf *Raft) AssembleVoteRequest() (*RequestVoteArgs)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	request_vote := &RequestVoteArgs{}
	curLogIndex, curLogTerm  := rf.GetLogState()


	request_vote.LastLogTerm = curLogTerm
	request_vote.Term = rf.persitent_state.currentTerm
	request_vote.CandidateId = rf.me
	request_vote.LastLogIndex = curLogIndex

	rf.persist()
	return request_vote
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// case where we don't grant the vote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.maybeUpdateTerm(args.Term)

	DPrintf("Server: %d (TERM: %d, LOG_IDX: %d Last_Log_Term: %d) Recieved vote request from: %d (TERM: %d, LOG_IDX: %d Last_Log_Term: %d )\n", rf.me, rf.persitent_state.currentTerm, rf.getLastIndex(), rf.getLastTerm(), args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
	if rf.persitent_state.votedFor == -1 || rf.persitent_state.votedFor == args.CandidateId {
		// If the logs have last entries with different terms, then
		//the log with the later term is more up-to-date. If the logs
		//end with the same term, then whichever log is longer is
		//more up-to-date.
		if args.LastLogTerm > rf.getLastTerm() || (args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex()) {
			DPrintf("Server: %d Granted the vote\n", rf.me)

			reply.VoteGranted = true
			rf.last_synced_time = time.Now() // this server is visited by a leader or candidate. Update this
			reply.Term = rf.persitent_state.currentTerm
			rf.persitent_state.votedFor = args.CandidateId
			rf.persist()
			return
		}
	}
	reply.Term = rf.persitent_state.currentTerm
	reply.VoteGranted = false
	DPrintf("Server: %d Rejected the vote\n", rf.me)
	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


