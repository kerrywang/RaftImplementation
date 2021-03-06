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
	LastLogTerm int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// case where we don't grant the vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server: %d (TERM: %d, LOG_IDX: %d Last_Log_Term: %d) Recieved vote request from: %d (TERM: %d, LOG_IDX: %d Last_Log_Term: %d )\n", rf.me, rf.currentTerm, rf.getLastIndex(), rf.getLastTerm(), args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	if (args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		rf.HasLaterLog(args.LastLogIndex, args.LastLogTerm)) {
		DPrintf("Server: %d Rejects the vote\n", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.currentTerm = MAX(rf.currentTerm, args.Term)
	} else {
		DPrintf("Server: %d Granted the vote\n", rf.me)

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.curState = Follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.lastVisitedTime = time.Now() // this server is visited by a leader or candidate. Update this

	}
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


